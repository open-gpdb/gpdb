## TRY_CONVERT

TRY_CONVERT is Greenplum extension, which adds function for error-safe type cast like [TRY_CAST from SQL-Server](https://learn.microsoft.com/ru-ru/sql/t-sql/functions/try-cast-transact-sql?view=sql-server-ver16)


### Error handling

To handle errors from type cast we use "soft" error handling concept, introduced Postgres 17 (https://github.com/postgres/postgres/commit/ccff2d20ed9622815df2a7deffce8a7b14830965), that concept we spreaded on data types in [21be368
Preview](https://github.com/open-gpdb/gpdb/commit/21be3688729ec4468ffd083da197721860fa2cbd) and [d31f362
](https://github.com/open-gpdb/gpdb/commit/d31f362250105e456961c2c9249693e42e67eca9) commits.

### Usage

```
TRY_CONVERT(SOURCE_VALUE, DEFAULT_VALUE::TARGET_TYPE) 
    returns (VALUE_IN_TARGET_TYPE or DEFAULT_VALUE)
```

```
TRY_CONVERT('42'::text, NULL::int2) -- returns 42::int2
TRY_CONVERT('42d'::text, NULL::int2) -- returns NULL::int2
TRY_CONVERT('42d'::text, 1234::int2) -- returns 1234::int2
```

### Extension's type casts

Casting from extensions types is able only for extensions:

- hstore
- citext

To enable casting from hstore and citext types use, `add_type_for_try_convert(regtype)` function

#### Why signature is so strange?

Greenplum function polymorphism accept to have polymorphic functions only one any type in signature.  

#### Supported casts

    ✅ Values Cast
    ✅ Types with typemod
    ❌ Array-Array Cast
    ❌ To Domain type cast

