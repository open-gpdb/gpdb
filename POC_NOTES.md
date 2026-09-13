# POC: бескаталожные временные таблицы через CTAS (`gp_enable_catalogless_temp`)

Прототип «бескаталожных» временных таблиц для `CREATE TEMP TABLE ... AS
SELECT`. Цель ветки — оценить объём кода, а не получить готовую фичу.
Изначально код только компилировался; позже прототип был поднят на живом
demo-кластере и проверен смоук- и нагрузочным тестами (см. разделы про
тесты ниже).

## Реализованная архитектура

1. **Per-object триггер** (изменено с изначального сессионного GUC по
   фидбеку пользователя): WITH-опция самого CTAS —

   ```sql
   CREATE TEMP TABLE t WITH (catalogless[=true|false]) AS SELECT ...
   ```

   DefElem разбирается через `defGetBoolean` в `ExecCreateTableAs`
   (краткая форма `WITH (catalogless)` = true) и там же **поглощается**
   (удаляется из `into->options`) в обоих случаях — и true, и false, —
   поэтому обычный путь CTAS никогда не видит неизвестный reloption, а
   `WITH (catalogless=false)` создаёт совершенно обычную temp-таблицу.
   `catalogless` на не-TEMP таблице даёт `ERROR: catalogless requires
   TEMP`; на matview, `WITH NO DATA` и `ON COMMIT` — чистые ошибки «не
   реализовано».

   **Решение по GUC**: `gp_enable_catalogless_temp` (bool, PGC_USERSET,
   зарегистрирован в guc_gp.c + sync_guc_name.h, переменная в
   cdbtempresult.c) оставлен как **глобальный kill-switch, по умолчанию
   on**; при off опция даёт `ERROR: catalogless temp tables are
   disabled`. Оставлен, а не удалён, потому что (а) прод-фиче такой
   инвазивности нужен ops-уровневый выключатель, не требующий правок
   приложений, и (б) read-path (поиск в реестре в `addRangeTableEntry` /
   `RemoveRelations`) уже использует его как дешёвый guard — удаление
   тронуло бы больше кода при строго худшей безопасности. Сам по себе
   GUC больше ничего не маршрутизирует: без WITH-опции поведение любых
   стейтментов не меняется.

2. **Сессионный реестр** — `src/backend/cdb/cdbtempresult.c`,
   `src/include/cdb/cdbtempresult.h`. Per-session хеш
   `имя -> {virtual_id, TupleDesc, GpPolicy, rowcount, NTupleStore}` в
   TopMemoryContext, существует на QD и на QE. Virtual id — сессионный
   счётчик, назначается на QD и доезжает до QE внутри `IntoClause`.

3. **Путь записи** — `src/backend/commands/createas.c`:
   - `ExecCreateTableAs` помечает `IntoClause`
     (`isTempResult`/`tempResultId`, новые поля сериализованы в
     out/read/copy/equal-функциях), когда задана WITH-опция
     `catalogless` (и kill-switch это разрешает) на цели с
     `RELPERSISTENCE_TEMP`.
   - `intorel_initplan` сворачивает в `tempresult_initplan`: **ни одной
     строки каталога** (ни `DefineRelation`/`heap_create_with_catalog`,
     ни toast/aoseg, ни relfilenode). Регистрирует запись; на QE создаёт
     сегмент-локальный writer `NTupleStore`
     (`ntuplestore_create_readerwriter`) с детерминированным именем
     `CTMPRES_<gp_session_id>_<virtual_id>` (по образцу
     `shareinput_create_bufname_prefix`).
   - `intorel_receive`/`intorel_shutdown` дописывают слоты в tuplestore /
     сбрасывают его; store остаётся открытым до конца транзакции.
   - QD после `ExecutorEnd` записывает суммарный rowcount
     (`es_processed`) в свою запись реестра.

4. **Путь чтения** — `src/backend/parser/parse_relation.c`:
   `addRangeTableEntry` резолвит неквалифицированное имя по реестру *до*
   каталога и строит RTE нового вида `RTE_TEMPRESULT`
   (`addRangeTableEntryForTempResult`). RTE переиспользует CTE-поля
   (`ctename`, `ctecoltypes/typmods/collations`), благодаря чему
   `expandRTE`, `get_rte_attribute_type` и пр. работают по CTE-веткам
   (паттерн: минимальный порт `RTE_NAMEDTUPLESTORE` из PG10, без
   QueryEnvironment).

5. **Планировщик** — новый plan-узел `TempResultScan` + путь:
   - `relnode.c`: `build_simple_rel` заполняет атрибуты и
     `rel->cdbpolicy` из политики в реестре.
   - `allpaths.c`: `set_tempresult_size_estimates` (точный rowcount из
     реестра) и `set_tempresult_pathlist`.
   - `pathnode.c`: `create_tempresultscan_path`; locus через
     `cdbpathlocus_from_baserel` поверх `GpPolicy` из реестра — temp
     result с hash-распределением джойнится по своему ключу
     распределения без Motion.
   - `costsize.c`: `cost_tempresultscan` (клон `cost_valuesscan`).
   - `createplan.c`: `create_tempresultscan_plan`/`make_tempresultscan`;
     plan-узел несёт `tsname`, `tempresid` и списки типов колонок, чтобы
     любой executor-процесс мог восстановить TupleDesc скана без доступа
     к каталогу/реестру.
   - `setrefs.c`, `subselect.c`, `cdbplan.c`, `cdbmutate.c`,
     `cdbllize.c`, `cdbpath.c`, `cdbtargeteddispatch.c`, `walkers.c`:
     обвязочные кейсы.
   - **Fallback с ORCA**: `standard_planner` обходит запрос в поисках
     `RTE_TEMPRESULT` и пропускает `optimize_query` (принудительный
     fallback на планировщик Postgres) — у DXL-транслятора нет
     представления для нового вида RTE.

6. **Executor** — `src/backend/executor/nodeTempResultScan.c`
   (+ заголовок): лениво открывает локальный ntuplestore при первом
   fetch'е (по имени, reader-режим) и отдаёт кортежи через `ExecScan`.
   Обвязка в `execProcnode.c`, `execAmi.c` (rescan — seek на BOF),
   `explain.c` («Temp Result Scan»).

7. **Сериализация** — `outfuncs.c`/`outfast.c`, `readfuncs.c`/
   `readfast.c`, `copyfuncs.c`, `equalfuncs.c` для `TempResultScan`,
   полей `IntoClause` и кейса `RTE_TEMPRESULT`.

8. **Очистка** — `RegisterXactCallback` в `cdbtempresult.c`: на
   commit/abort верхнего уровня все tuplestore (writer и отслеживаемые
   reader'ы) уничтожаются, реестр очищается. Подтранзакции: регистрация
   внутри subxact даёт `ERROR` (рамки POC).
   `DROP TABLE <имя>` перехватывается в начале `RemoveRelations`
   (`tablecmds.c`) и удаляет запись локально.

## Застаблено / даёт ошибку (ERRCODE_FEATURE_NOT_SUPPORTED)

- `WITH NO DATA` (`ExecCreateTableAs`)
- клаузы `ON COMMIT ...`
- `CREATE TABLE AS EXECUTE`
- бескаталожная temp-таблица внутри подтранзакции (в момент регистрации)
- `SELECT ... FOR UPDATE/SHARE` по temp result (`analyze.c`)

## Не обработано вовсе (имя просто не резолвится / каталожная ошибка)

- INSERT/UPDATE/DELETE в temp result (резолв идёт через
  `setTargetTable` -> каталог -> «relation does not exist»)
- индексы, ALTER, `\d` в psql, pg_dump, COPY, ANALYZE, VACUUM
- обычный `CREATE TEMP TABLE` без `AS`

## Известные дыры / читы

- **Reader-ганги**: в смоук-тесте работает (self-join читает store из
  reader-ганга): детерминированное имя файла в общем каталоге
  `base/pgsql_tmp` сегмента делает store находимым из любого процесса
  того же сегмента, а TupleDesc скана восстанавливается из плана, так
  что доступ к реестру для чтения не нужен. Осталось необработанным:
  сегмент, получивший **ноль** строк, вообще не создаёт файлов, и скан
  там упадёт на open (на тестовых распределениях не проявилось).
- Нет синхронизации между writer'ом CTAS и последующими reader'ами
  (ShareInputScan-протокол ready/done через FIFO не портирован);
  безопасно только потому, что стейтменты внутри сессии сериализованы.
- Store'ы открываются с interXact = true и **без** учёта в workfile
  manager (`ntuplestore_create_readerwriter_xact`), поэтому
  gp_workfile_*-представления их не видят и per-query учёт спилла не
  применяется.
- `DROP TABLE` на QD удаляет только запись QD; store'ы на QE живут до
  конца транзакции. Стейтмент в этом случае не диспетчеризуется.
- Чтение temp result, в который на данном сегменте не было записи
  (0 строк там), всё равно попытается открыть файл — вероятна ошибка в
  рантайме; нужна обработка «нет файла = пустой store».
- Затенение: имя из реестра затеняет любую каталожную таблицу с тем же
  именем при неквалифицированном чтении; правила взаимодействия не
  проектировались.
- `EXPLAIN (VERBOSE)`/deparse в ruleutils обработаны минимально.
- Учёт памяти переиспользует owner-тег ValuesScan для нового узла.
- Поддержка mark/restore для `TempResultScan` не добавлена.

## Фиксы окружения (к самому POC отношения не имеют)

- `src/backend/cdb/motion/ic_udpifc.c`: свежие macOS SDK больше не
  определяют `HZ` в `<sys/param.h>`; добавлен `#ifndef HZ #define HZ
  100`.
- `src/backend/gporca/gporca.mk`: заголовки homebrew xerces-c используют
  конструкции C++11; с `-std=gnu++98 -Werror -Wpedantic` сборка ORCA
  падает. Добавлены `-Wno-c++11-extensions -Wno-long-long`.

## Оценка оставшейся работы до продакшена

- Корректная межганговая/межслайсовая видимость store'ов (проблема
  reader-гангов выше); скорее всего нужна синхронизация writer/reader в
  духе ShareInputScan и семантика «нет файла = пусто»: **крупно**.
- DML (INSERT/UPDATE/DELETE) или хотя бы чистые ошибки для него.
- Поддержка ORCA (новый DXL-оператор или честная аннотация fallback'а).
- Учёт спилла/памяти, интеграция с workfile manager, statement_mem.
- Взаимодействие с каталожными именами, семантика search_path, EXPLAIN,
  ruleutils, представления поверх temp result'ов, обработка pg_temp.
- Подтранзакции/savepoints, семантика ON COMMIT, взаимодействие
  xact-callback'а с 2PC.
- Тесты (regress + isolation), поддержка \d, определение поведения
  pg_dump.

Грубая оценка: POC — это ~10–15% продакшен-реализации.

## Живой смоук-тест (2026-08-31)

Прогнан на 2-сегментном gpdemo-кластере, собранном из этой ветки (macOS
arm64): `test/catalogless_smoke.sql`, полный вывод в
`test/catalogless_smoke.out`.

Проверено:

1. **Ноль строк каталога**, пока бескаталожная temp-таблица жива:
   счётчики `pg_class`/`pg_attribute`/`pg_type`/`pg_depend` байт-в-байт
   совпадают с базовой линией на QD и обоих сегментах
   (438/3376/441/8072), и строки `poc_t` в `pg_class` нет нигде.
   Контрольный CTAS без бескаталожного пути (теперь проверяется как
   `WITH (catalogless=false)`, которая должна поглощаться, а не попадать
   в reloptions-валидацию) добавляет +1/+9/+2/+3 строки на QD и каждом
   сегменте и строку в `pg_class` создаёт.
2. **Чтение работает и планируется правильно**: `count/sum` по 1000
   строк верны (1000/1001000); WHERE + ORDER BY верны; join с
   co-distributed heap-таблицей показывает `Temp Result Scan` с
   co-located Hash Join и **без Redistribute Motion** (hashed locus из
   сохранённой политики, rows=500/сегмент из точного rowcount);
   self-join повторно читает tuplestore (reader-гангом) корректно.
3. **Транзакционный scope**: после COMMIT имя больше не резолвится, а
   файлы `pgsql_tmp_CTMPRES_*` удалены с обоих сегментов (проверено:
   есть во время транзакции, нет после).
4. **Негативы**: INSERT и CREATE INDEX по бескаталожной temp-таблице
   падают с чистой ошибкой `relation ... does not exist` (имя невидимо
   для каталожного резолва DML/DDL) — без крэшей. DROP TABLE удаляет
   запись. С per-object синтаксисом покрыты ещё два кейса:
   `CREATE TABLE ... WITH (catalogless=true)` (без TEMP) →
   `ERROR: catalogless requires TEMP`; опция при
   `gp_enable_catalogless_temp = off` → `ERROR: catalogless temp tables
   are disabled` (оба зафиксированы в `test/catalogless_smoke.out`).

Баги, найденные и починенные при поднятии (отдельные коммиты):

- GUC отсутствовал в `sync_guc_name.h` → FATAL сервера на старте.
- Файлы tuplestore открывались с interXact = false → закрывались и
  удалялись resource owner'ом в конце создающего стейтмента; добавлен
  `ntuplestore_create_readerwriter_xact` (interXact = true, жизненным
  циклом владеет реестр).
- `find_indexkey_var` сваливался в поиск по pg_attribute с relid 0 для
  колонки ключа распределения, отсутствующей в reltargetlist
  (`SELECT count(*)`) → типы берутся из RTE.

## Нагрузочный тест (2026-08-31)

`test/catalogless_bigdata.sql` (вывод: `test/catalogless_bigdata.out`),
тот же 2-сегментный demo-кластер. Readerwriter-NTupleStore всегда
файловый и при maxBytes = 0, как передаёт POC, держит в памяти окно
всего в 16 страниц по 32KB = **512KB на store**; тест гоняет
вытеснение/подкачку сильно за его пределами. Все фазы прошли, чинить
ничего не пришлось:

1. **20M узких строк** (`id int, v bigint`): CTAS 14.3s, спилл-файл
   `pgsql_tmp_CTMPRES_*` = **268MB на сегмент**. `count/sum` точны
   (20000000 / 1400000070000000 = 7·n·(n+1)/2), выборочный `WHERE id
   IN` точен, повторный полный `count(*)` (чистое перечтение с диска)
   1.5s.
2. **RSS writer-QE** (семплы каждые 4s): 84MB до, **119MB плоско в
   течение всей записи 268MB**, 119.6MB после чтений — память не растёт
   с объёмом данных, есть только окно 512KB + оверхед motion/executor.
   (Поздний пик 224MB принадлежит hash-таблице джойна — 500K строк heap
   на сегмент, — а не tuplestore.)
3. **LOB-путь**: при BLCKSZ=32768 NTS_MAX_ENTRY_SIZE ≈ 32700 байт,
   поэтому предложенный 2KB-pad остался бы inline; взят
   `repeat('x',40000)` (кортежи 40KB, 20k строк). `_LOB`-файл =
   **381–383MB на сегмент**, `count`/`sum(length)`/выборочные длины
   точны. LOB-пути записи и чтения работают без правок.
4. **Join на объёме** (20M temp result × 1M co-distributed heap,
   count = 1000000 в обоих вариантах):
   - catalogless (Temp Result Scan, планировщик Postgres из-за
     принудительного fallback'а): EXPLAIN ANALYZE 4.9s, скан 10M
     строк/сегмент 1.49s;
   - контрольная heap temp-таблица (план выбрала ORCA): EXPLAIN ANALYZE
     5.3s, Seq Scan 1.8s; её CTAS занял 16.5s против 14.3s.
   Т.е. чтение 20M строк из ntuplestore на уровне heap-скана тех же
   данных (здесь даже чуть быстрее); сборка -O0, один хост — считать
   сравнением смоук-уровня.
5. **COMMIT**: `pgsql_tmp` на обоих сегментах пуст, `find *CTMPRES*` =
   0.

Замечание про окно 512KB: оно ограничивает только page-cache каждого
store (скорость записи / локальность перечтений), не корректность. При
развитии окно стоит задавать как у других операторов — либо из
`statement_mem`/operator memory (сторона записи уже получает
`PlanStateOperatorMemKB`, но reader в POC передаёт maxBytes = 0), либо
отдельным GUC; большее окно помогает в основном повторным мелким
range-сканам, последовательные полные сканы и так в порядке.

## Как поднять demo-кластер

У worktree свой install-префикс (GPHOME пользователя не трогается):
configure с `--prefix=/Users/alena/open-gpdb3-poc/idea1/install`
(остальные опции как в основном дереве) и:

```sh
cd /Users/alena/open-gpdb3-poc/idea1
make -j8 -C src/backend install
make -C gpMgmt/bin psutil pyyaml CC="gcc -Wno-error=implicit-function-declaration"  # psutil 5.7.0 vs новый clang
make -C gpMgmt install
make -C gpcontrib/gp_internal_tools install    # gp_resource_group.so нужен initdb

export GPHOME=/Users/alena/open-gpdb3-poc/idea1/install
source $GPHOME/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

# первичная инициализация (нестандартные порты, чтобы не задеть кластеры пользователя):
cd gpAux/gpdemo
export DEMO_PORT_BASE=16432 NUM_PRIMARY_MIRROR_PAIRS=2 WITH_MIRRORS=false \
       DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs
bash demo_cluster.sh        # финальный gpstart внутри gpinitsystem может упасть; тогда:
pg_ctl -D $MASTER_DATA_DIRECTORY stop -m fast   # остановить utility-мастер, который он оставил
gpstart -a

# дальше обычные запуск/остановка:
gpstart -a
gpstop -a

# прогон смоук-теста:
createdb -p 16432 pocdb
DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs \
  psql -p 16432 pocdb -e -f test/catalogless_smoke.sql
```

Кластер: master :16432, primary :16434/:16435, данные в
`gpAux/gpdemo/datadirs` внутри worktree. Каталоги сборки `install/`,
`gpAux/gpdemo/datadirs/` и распакованные `gpMgmt/bin/pythonSrc/ext/*`
намеренно оставлены untracked.

Особенности окружения, встреченные при поднятии (задокументированы, это
не баги POC): top-level `make install` падает в `contrib/hstore` на этом
SDK (не нужен); `gpstart` требует python2 `psutil`, исходникам 5.7.0
нужен `-Wno-error=implicit-function-declaration` с текущим clang.

## Тест планов (2026-09-13)

`test/catalogless_plans.sql` (вывод: `test/catalogless_plans.out`):
шесть форм запросов по catalogless-таблицам, каждый с EXPLAIN и
побитовой сверкой count/sum с обычными temp-двойняшками тех же данных;
для каждой созданной таблицы — дельта pg_class/pg_attribute/pg_type/
pg_depend на QD и обоих сегментах (итоговая таблица в выводе:
catalogless — строго 0 везде, обычная temp — +1/+10/+2/+3 на узел).

1. join по ключу распределения — Temp Result Scan без Redistribute;
2. join по не-ключу — планировщик ставит Broadcast Motion (стороны не
   считаются co-located ошибочно);
3. GROUP BY по не-ключу — двухфазный HashAggregate с Redistribute
   поверх Temp Result Scan;
4. join двух catalogless-таблиц с разными ключами — сторона с
   совпадающим ключом на месте, вторая перераспределяется;
5. EXISTS (semi-join) — работает после фикса: `pathnode_walk_kids`
   (cdbpath dedup) не знал T_TempResultScan и падал с
   "unrecognized path type: 126"; добавлен в список листовых путей;
6. ORDER BY + LIMIT — Sort+Limit на сегментах, Gather, финальный Limit.

Все шесть результатов совпали с контролем бит-в-бит.

## Diffstat

(относительно OPENGPDB_STABLE; `git diff OPENGPDB_STABLE --stat | tail -5`)

```
 test/catalogless_bigdata.out              | 178 +++++++++++++++++
 test/catalogless_bigdata.sql              |  90 +++++++++
 test/catalogless_smoke.out                | 278 ++++++++++++++++++++++++++
 test/catalogless_smoke.sql                | 168 ++++++++++++++++
 59 files changed, 2559 insertions(+), 8 deletions(-)
```

Сборка: configure с теми же опциями, что у основного дерева
(`--with-perl --without-openssl --without-gssapi --with-libxml
--without-mdblocales --without-zstd --without-python --without-icu
CFLAGS/CXXFLAGS='-O0 -g3'`); `make -j8 -C src/backend` проходит и
линкует бинарник `postgres` (включая ORCA, с двумя фиксами окружения
выше).
