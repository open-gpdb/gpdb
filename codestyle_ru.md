# Требования к оформлению кода

Написание кода должно быть оптимизировано в пользу:
- единообразия,
- ускорения чтения и понимания,
- сокращения вероятности ошибок при дальнейшем сопровождении,
- обнаружения ошибок на этапе компиляции, а не более поздних этапах.

## Эти проверки должны автоматически проходить на гитхабе:
1. Компиляция с флагом `-Werror` должна проходить успешно.
2. clang-format (для Orca) и pgindent (для остального кода) не должны обнаруживать ошибок оформления.
3. В конце строки не должно быть лишних пробельных символов.
4. Файл должен заканчиваться на единственный перевод строки.

## Для исключения ошибок или обнаружения их на этапе компиляции:
**1.** Везде, где возможно, использовать `= {0}` вместо `memset()` и присваивание вместо `memcpy()`. Так код будет более понятным (не требуется помнить, в какой аргумент передается источник, а в какой получатель), будет меньше мест, где можно случайно ошибиться (соответствие типов и размеров), и генерируется более оптимальный машинный код, так как компилятор не обязан делать вызов функции.

```c
typedef struct S1 {
	int i;
	char* s;
} S1;

void f(void)
{
	S1 s11 = {0};
	S1 s12 = s11;
}
```

Ассемблерный код зануления и присваивания

```asm
movq	$0, -32(%rbp)
movq	$0, -24(%rbp)
movq	-32(%rbp), %rax
movq	-24(%rbp), %rdx
movq	%rax, -16(%rbp)
movq	%rdx, -8(%rbp)
```

В случае несоответствия типов ошибка будет обнаружена на этапе компиляции

```c
typedef struct S1 {
	int i;
	char* s;
} S1;
typedef struct S2 {
	char* s;
	int i;
	char c;
} S2;

void f(void)
{
	S1 s11 = {0};
	S2 s2 = s11;
}
```

```
--------------------
1.c: In function ‘f’:
1.c:14:17: error: invalid initializer
   14 |         S2 s2 = s11;
      |                 ^~~
```

В приведенном ниже коде есть ошибки несоответствия размеров и типов в вызовах `memset` и `memcpy`, но компилятор не может их обнаружить.

```c
#include <string.h>
typedef struct S1 {
	int i;
	char* s;
} S1;
typedef struct S2 {
	char* s;
	int i;
	char c;
} S2;

void f(void)
{
	S1 s11;
	S2 s2;

	memset(&s11, 0, sizeof(S2));	
	memcpy(&s2, &s11, sizeof(s2));
}
```

**2.** По возможности не использовать `void*` и преобразование типов, чтобы соответствие типов проверялось при компиляции.

```c
typedef struct Context Context;

Context* getContext(void);
void useContext(Context* cxt);

void f(void)
{
	Context* cxt = getContext();
	useContext(cxt);
}
```

В случае опечатки `useContext(&cxt);` компилятор выдаст предупреждение

```
1.c: In function ‘f’:
1.c:9:20: warning: passing argument 1 of ‘useContext’ from incompatible pointer type [-Wincompatible-pointer-types]
    9 |         useContext(&cxt);
      |                    ^~~~
      |                    |
      |                    Context **
1.c:4:26: note: expected ‘Context *’ but argument is of type ‘Context **’
    4 | void useContext(Context* cxt);
      |                 ~~~~~~~~~^~~
```

При использовании `void*` компилятор не выдаст предупреждения

```c
void* getContext(void);
void useContext(void* cxt);

void f(void)
{
	void* cxt = getContext();
	useContext(&cxt);
}
```

**3.** Использовать атрибуты формата при объявлении функции, если функция подобна `printf`. Так на этапе компиляции будет проверяться соответствие аргументов строке формата.

```c
extern int
errmsg(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

void f(void)
{
	int i = 0;
	errmsg("%s\n", i);
}
```

В данном коде в строке формата указана строка, а передается число. Будет выдано предупреждение

```
1.c: In function ‘f’:
1.c:10:18: warning: format ‘%s’ expects argument of type ‘char *’, but argument 2 has type ‘int’ [-Wformat=]
   10 |         errmsg("%s\n", i);
      |                 ~^     ~
      |                  |     |
      |                  |     int
      |                  char *
      |                 %d
```

**4.** Если функция или переменная используются за пределами файла, в котором определены, то должно быть объявление в заголовочном файле, который должен включаться в файл с определением и файлы с использованием. Так исключаются ошибки несогласованного изменения типа переменной, а также типов и количества аргументов функции.

файл 1.c содержит определение функции

```c
int f(unsigned char v)
{
	return 1000 + v;
}
```

файл 2.с содержит объявление и вызов функции, но объявление отличается от определения

```c
#include <stdio.h>

int f(int v);

int main()
{
	printf("%d\n", f(1000));
	return 0;
}
```

Компиляция и запуск

```
$ gcc 1.c 2.c && ./a.out 
1232
```

Ожидалось, что программа выдаст 1000+1000 = 2000, а по факту неожиданно было выдано 1232. Причина в том, что в определении и объявлении функции отличает тип аргумента.

При объявлении функции в заголовочном файле ошибка обнаруживается при компиляции.

```
$ cat 1.c 
int f(unsigned char v)
{
	return 1000 + v;
}

$ cat 1.h
int f(unsigned char v);

$ cat 2.c 
#include <stdio.h>
#include "1.h"

int main()
{
	printf("%d\n", f(1000));
	return 0;
}

$ gcc 1.c 2.c
2.c: In function ‘main’:
2.c:6:26: warning: unsigned conversion from ‘int’ to ‘unsigned char’ changes value from ‘1000’ to ‘232’ [-Woverflow]
    6 |         printf("%d\n", f(1000));
      |                          ^~~~
```

**5.** Использовать функции, контролирующие переполнение (`snprintf` вместо `sprintf`, `strlcpy` вместо `strcpy`), если переполнение не исключено или может произойти в результате вероятных будущих изменений, например в вызывающей функции.

**6.** Использовать `static` со всеми функциями и переменными, которые не используются вне модуля. Так будут исключены ошибки с ABI, если функция и переменная будут использоваться в стороннем расширении, а потом изменятся без перекомпиляции этого расширения.

**7.** Вместо макросов, если это возможно, должны использоваться перечисления, `static const char*`, функции со спецификаторами `static inline`. Так будет меньше возможных ошибок, больше проверок будет выполнено при компиляции и проще пользоваться отладчиком.

```c
#include <stdio.h>
#define LEN 15 + 1
int main()
{
	printf("%d\n", LEN * 2);
	return 0;
}
```

Программа выдаст 17, потому что в выражении `15 + 1 * 2` умножение приоритетнее. Необходимо помнить, что выражение надо обернуть в скобки

Отладчик не знает про `LEN`

```
(gdb) p LEN 
No symbol "LEN" in current context.
```

Если заменить макрос на перечисление

```c
#include <stdio.h>
enum {LEN = 15 + 1};
int main()
{
	printf("%d\n", LEN * 2);
	return 0;
}
```

Программа выдаст 32, потому что `LEN` всегда 16 независимо от контекста использования.
В отладчике можно узнать значение `LEN`.

```
Breakpoint 1, main () at 1.c:5
5		printf("%d\n", LEN * 2);
(gdb) p (int)LEN 
$2 = 16
(gdb) 
```

Строковый макрос не виден в отладчике

```c
#include <stdio.h>
#define STR "some string"
int main()
{
	puts(STR);
	return 0;
}
```

```
Breakpoint 1, main () at 1.c:5
5		puts(STR);
(gdb) p STR
No symbol "STR" in current context.
(gdb) 
```

Строка, объявленная через `const char*`, видна

```c
#include <stdio.h>
const char* STR = "some string";
int main()
{
	puts(STR);
	return 0;
}
```

```
Breakpoint 1, main () at 1.c:5
5		puts(STR);
(gdb) p STR
$1 = 0x555555556004 "some string"
(gdb) 
```

На макрос нельзя поставить точку останова

```c
#include <stdio.h>
#include <ctype.h>

#define ISWORDCHR(c)	(isalpha(c) || isdigit(c))

int main(int argc, char** argv)
{
	if (ISWORDCHR(argv[1][0]))
		puts("Yes");
	return 0;
}
```

```
(gdb) b ISWORDCHR
Function "ISWORDCHR" not defined.
Make breakpoint pending on future shared library load? (y or [n]) 
```

При замене макроса на функцию

```c
#include <stdio.h>
#include <ctype.h>
#include <stdbool.h>

static inline bool ISWORDCHR(char c)
{
	return isalpha(c) || isdigit(c);
}

int main(int argc, char** argv)
{
	if (ISWORDCHR(argv[1][0]))
		puts("Yes");
	return 0;
}
```

точка останова на ISWORDCHR ставится

```
(gdb) b ISWORDCHR
Breakpoint 1 at 0x117a: file 1.c, line 7.
(gdb) r test
Starting program: /tmp/a.out test
[Thread debugging using libthread_db enabled]
Using host libthread_db library "/lib/x86_64-linux-gnu/libthread_db.so.1".

Breakpoint 1, ISWORDCHR (c=116 't') at 1.c:7
7		return isalpha(c) || isdigit(c);
(gdb) 
```

Если `ISWORDCHR(argv[1][0])` заменить на `ISWORDCHR(argc)`, то в случае функции будет предупреждение компилятора о несоответствии типов

```
$ gcc  1.c -Wconversion
1.c: In function ‘main’:
1.c:12:23: warning: conversion from ‘int’ to ‘char’ may change value [-Wconversion]
   12 |         if (ISWORDCHR(argc))
      |                       ^~~~
```

При использовании макроса предупреждение выдано не будет, хотя передано количество аргументов вместо символа в одном из них.


**8.** Если функция не принимает аргументы, то в объявлении функции в круглых скобках должно быть написано `void`, чтобы компилятор проверял, что аргументы не передаются.

```
$ cat 1.h 
void f(void);

$ cat 2.c
#include "1.h"

int main()
{
	f(10);
	return 0;
}

$ gcc -c 2.c
2.c: In function ‘main’:
2.c:5:9: error: too many arguments to function ‘f’
    5 |         f(10);
      |         ^
In file included from 2.c:1:
1.h:1:6: note: declared here
    1 | void f(void);
      |      ^
```

**9.** По возможности переменная должна быть инициализирована при объявлении или объявлена как можно ближе к месту ее инициализации. Это исключает возможность чтения из неинициализированной переменной. До PostgreSQL 12 такой подход был запрещен для обеспечения совместимости со старыми компиляторами. Для нас это ограничение не актуально.


## Читаемость:
**1.** Комментарии к функциям должны соответствовать коду функции, правилам правописания, не выходить за границу 80 символов.

**2.** Имена переменных и функций должны соответствовать их предназначению.

**3.** При ветвлении первой желательно размещать ветку с меньшим количеством строк. Чем меньше строк разделяют `if` и `else`, тем проще понять, к какому `if` относится `else`. Но обработку ошибки следует размещать в `else`, потому что обработка ошибки - это маловероятная ситуация, значит обычно хочется читать код, не относящийся к обработке ошибки, и его будет легче читать, если он написан единым фрагментом.

В данном фрагменте кода следует инвертировать условие

```c
if (fstuple != NULL)
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("found two entries in \"%s\" with segno %d: "
					"(ctid %s with eof " INT64_FORMAT ") and (ctid %s with eof " INT64_FORMAT ")",
					RelationGetRelationName(pg_aoseg_rel),
					segno,
					ItemPointerToString(&fstuple->t_self),
					DatumGetInt64(
								  fastgetattr(fstuple, Anum_pg_aoseg_eof, pg_aoseg_dsc, &isNull)),
					ItemPointerToString2(&tuple->t_self),
					DatumGetInt64(
								  fastgetattr(tuple, Anum_pg_aoseg_eof, pg_aoseg_dsc, &isNull)))));
else
	fstuple = heap_copytuple(tuple);
```

Получится код, в котором надо прочитать меньше строк, чтобы найти `else`, и обработка ошибка будет находиться в `else`.

```c
if (fstuple == NULL)
	fstuple = heap_copytuple(tuple);
else
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("found two entries in \"%s\" with segno %d: "
					"(ctid %s with eof " INT64_FORMAT ") and (ctid %s with eof " INT64_FORMAT ")",
					RelationGetRelationName(pg_aoseg_rel),
					segno,
					ItemPointerToString(&fstuple->t_self),
					DatumGetInt64(
								  fastgetattr(fstuple, Anum_pg_aoseg_eof, pg_aoseg_dsc, &isNull)),
					ItemPointerToString2(&tuple->t_self),
					DatumGetInt64(
								  fastgetattr(tuple, Anum_pg_aoseg_eof, pg_aoseg_dsc, &isNull)))));
```

**4.** Область видимости переменной должна быть минимальной. Так требуется просматривать меньший фрагмент кода, чтобы разобраться, где и для чего используется переменная.

Если переменная `i` используется только внутри блока `if`, то ее следует объявлять не на уровне функции

```c
void f(void)
{
	int i;
	.........
	if (.....)
	{
		i = 0;
		......
	}
}
```

а на под `if`.

```c
void f(void)
{
	.........
	if (.....)
	{
		int i = 0;
		......
	}
}
```

**5.** Если значение не меняется, должен использоваться спецификатор `const`. Это дает уверенность, что значение нигде не изменится после инициализации, и, как следствие, не требуется внимательно читать все места, где используется переменная. Особенно это полезно для аргументов функций, чтобы не было необходимости смотреть код функции на предмет, меняется ли в нем структура.

**6.** При проверке, является ли указатель нулевым, следует использовать сравнение с `NULL` вместо отрицания. Машинный код будет один и тот же, но зато при чтении можно понять, что переменная указатель, а не `bool`.

При чтении этого кода создается ложное впечатление, что `recoveryRestoreCommand` объявлена как `bool`.

```c
if (!recoveryRestoreCommand)
```

При такой записи видно, что переменная - указатель.

```c
if (recoveryRestoreCommand == NULL)
```

**7.** Если возможно, минимизировать вложенность.

Вместо

```c
void f(int i)
{
	if (i == 0)
	{
		......
		if (g())
		{
			......
		}
	}
}
```

следует писать

```c
void f(int i)
{
	if (i != 0)
		return;
	......
	if (!g())
		return;
	......
}
```
