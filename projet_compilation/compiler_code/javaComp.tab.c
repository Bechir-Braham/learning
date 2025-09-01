/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "javaComp.y"

     #include <stdio.h>
     #include <stdlib.h>
     #include <string.h>
     #include "semantic.c"
     #include "generator.c"
     void yyerror(char const *msg);	
     int yylex(void);
	extern int yylineno;
     extern int errorCount;
     extern int warningCount;
     int value;
     void beginSemantic();
     void endSemantic();
     void beginCodeGen();
     void endCodeGen();
     int current_index;
     int if_index;
     int saut_index;
     char current_id[256];
     code_line line;
     char name[256];
     char operator[10];

#line 96 "javaComp.tab.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "javaComp.tab.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_IDENTIFIER = 3,                 /* IDENTIFIER  */
  YYSYMBOL_TYPE_INT = 4,                   /* TYPE_INT  */
  YYSYMBOL_TYPE_BOOLEAN = 5,               /* TYPE_BOOLEAN  */
  YYSYMBOL_TYPE_STRING = 6,                /* TYPE_STRING  */
  YYSYMBOL_BOOLEAN_LITERAL = 7,            /* BOOLEAN_LITERAL  */
  YYSYMBOL_INTEGER_LITERAL = 8,            /* INTEGER_LITERAL  */
  YYSYMBOL_STRING_LITERAL = 9,             /* STRING_LITERAL  */
  YYSYMBOL_KEYWORD_CLASS = 10,             /* KEYWORD_CLASS  */
  YYSYMBOL_KEYWORD_PUBLIC = 11,            /* KEYWORD_PUBLIC  */
  YYSYMBOL_KEYWORD_STATIC = 12,            /* KEYWORD_STATIC  */
  YYSYMBOL_KEYWORD_VOID = 13,              /* KEYWORD_VOID  */
  YYSYMBOL_KEYWORD_MAIN = 14,              /* KEYWORD_MAIN  */
  YYSYMBOL_KEYWORD_EXTENDS = 15,           /* KEYWORD_EXTENDS  */
  YYSYMBOL_KEYWORD_RETURN = 16,            /* KEYWORD_RETURN  */
  YYSYMBOL_KEYWORD_IF = 17,                /* KEYWORD_IF  */
  YYSYMBOL_KEYWORD_ELSE = 18,              /* KEYWORD_ELSE  */
  YYSYMBOL_KEYWORD_WHILE = 19,             /* KEYWORD_WHILE  */
  YYSYMBOL_KEYWORD_PRINT = 20,             /* KEYWORD_PRINT  */
  YYSYMBOL_KEYWORD_NEW = 21,               /* KEYWORD_NEW  */
  YYSYMBOL_KEYWORD_THIS = 22,              /* KEYWORD_THIS  */
  YYSYMBOL_PARENTHESIS_OPEN = 23,          /* PARENTHESIS_OPEN  */
  YYSYMBOL_PARENTHESIS_CLOSE = 24,         /* PARENTHESIS_CLOSE  */
  YYSYMBOL_BRACKET_OPEN = 25,              /* BRACKET_OPEN  */
  YYSYMBOL_BRACKET_CLOSE = 26,             /* BRACKET_CLOSE  */
  YYSYMBOL_CURLY_BRACKET_OPEN = 27,        /* CURLY_BRACKET_OPEN  */
  YYSYMBOL_CURLY_BRACKET_CLOSE = 28,       /* CURLY_BRACKET_CLOSE  */
  YYSYMBOL_OP_AFFECT = 29,                 /* OP_AFFECT  */
  YYSYMBOL_OP_ADD = 30,                    /* OP_ADD  */
  YYSYMBOL_OP_SUBSTRACT = 31,              /* OP_SUBSTRACT  */
  YYSYMBOL_OP_MULTIPLY = 32,               /* OP_MULTIPLY  */
  YYSYMBOL_OP_NOT = 33,                    /* OP_NOT  */
  YYSYMBOL_LOG_AND = 34,                   /* LOG_AND  */
  YYSYMBOL_LOG_OR = 35,                    /* LOG_OR  */
  YYSYMBOL_LOG_LESS = 36,                  /* LOG_LESS  */
  YYSYMBOL_LOG_EQLESS = 37,                /* LOG_EQLESS  */
  YYSYMBOL_LOG_MORE = 38,                  /* LOG_MORE  */
  YYSYMBOL_LOG_EQMORE = 39,                /* LOG_EQMORE  */
  YYSYMBOL_LOG_EQ = 40,                    /* LOG_EQ  */
  YYSYMBOL_LOG_DIF = 41,                   /* LOG_DIF  */
  YYSYMBOL_SEMI_COLON = 42,                /* SEMI_COLON  */
  YYSYMBOL_DOT = 43,                       /* DOT  */
  YYSYMBOL_COMMA = 44,                     /* COMMA  */
  YYSYMBOL_YYACCEPT = 45,                  /* $accept  */
  YYSYMBOL_Program = 46,                   /* Program  */
  YYSYMBOL_MainClass = 47,                 /* MainClass  */
  YYSYMBOL_NestedClassDeclaration = 48,    /* NestedClassDeclaration  */
  YYSYMBOL_MainClassDeclaration = 49,      /* MainClassDeclaration  */
  YYSYMBOL_50_1 = 50,                      /* $@1  */
  YYSYMBOL_51_2 = 51,                      /* $@2  */
  YYSYMBOL_MainClassBody = 52,             /* MainClassBody  */
  YYSYMBOL_53_3 = 53,                      /* $@3  */
  YYSYMBOL_ClassDeclaration = 54,          /* ClassDeclaration  */
  YYSYMBOL_55_4 = 55,                      /* $@4  */
  YYSYMBOL_SimpleClassHeader = 56,         /* SimpleClassHeader  */
  YYSYMBOL_MultipleVariablesDeclaration = 57, /* MultipleVariablesDeclaration  */
  YYSYMBOL_SimpleVariableDeclaration = 58, /* SimpleVariableDeclaration  */
  YYSYMBOL_59_5 = 59,                      /* $@5  */
  YYSYMBOL_InlineVariables = 60,           /* InlineVariables  */
  YYSYMBOL_61_6 = 61,                      /* $@6  */
  YYSYMBOL_Variable = 62,                  /* Variable  */
  YYSYMBOL_MultipleMethodsDeclaration = 63, /* MultipleMethodsDeclaration  */
  YYSYMBOL_MethodDeclaration = 64,         /* MethodDeclaration  */
  YYSYMBOL_65_7 = 65,                      /* $@7  */
  YYSYMBOL_66_8 = 66,                      /* $@8  */
  YYSYMBOL_67_9 = 67,                      /* $@9  */
  YYSYMBOL_68_10 = 68,                     /* $@10  */
  YYSYMBOL_Type = 69,                      /* Type  */
  YYSYMBOL_MultipleStatements = 70,        /* MultipleStatements  */
  YYSYMBOL_Literal = 71,                   /* Literal  */
  YYSYMBOL_Statement = 72,                 /* Statement  */
  YYSYMBOL_73_11 = 73,                     /* $@11  */
  YYSYMBOL_74_12 = 74,                     /* $@12  */
  YYSYMBOL_75_13 = 75,                     /* $@13  */
  YYSYMBOL_76_14 = 76,                     /* $@14  */
  YYSYMBOL_77_15 = 77,                     /* $@15  */
  YYSYMBOL_Expression = 78,                /* Expression  */
  YYSYMBOL_79_16 = 79,                     /* $@16  */
  YYSYMBOL_80_17 = 80,                     /* $@17  */
  YYSYMBOL_CompositeExpression = 81,       /* CompositeExpression  */
  YYSYMBOL_82_18 = 82,                     /* $@18  */
  YYSYMBOL_83_19 = 83,                     /* $@19  */
  YYSYMBOL_MethodCall = 84,                /* MethodCall  */
  YYSYMBOL_MultipleExpressions = 85,       /* MultipleExpressions  */
  YYSYMBOL_86_20 = 86,                     /* $@20  */
  YYSYMBOL_Arithmetic_Operator = 87,       /* Arithmetic_Operator  */
  YYSYMBOL_Logical_Operator = 88           /* Logical_Operator  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  9
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   194

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  45
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  44
/* YYNRULES -- Number of rules.  */
#define YYNRULES  91
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  194

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   299


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,    81,    81,    83,    85,    86,    88,    88,    88,    90,
      90,    92,    92,    93,    95,    96,    98,    99,   101,   101,
     103,   103,   104,   105,   107,   109,   110,   112,   112,   112,
     112,   112,   115,   116,   117,   118,   120,   121,   122,   124,
     125,   126,   127,   129,   130,   131,   132,   132,   133,   133,
     134,   135,   137,   138,   135,   141,   143,   145,   145,   146,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   156,
     157,   158,   159,   159,   160,   160,   161,   163,   165,   165,
     166,   168,   169,   170,   172,   173,   174,   175,   176,   177,
     178,   179
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "IDENTIFIER",
  "TYPE_INT", "TYPE_BOOLEAN", "TYPE_STRING", "BOOLEAN_LITERAL",
  "INTEGER_LITERAL", "STRING_LITERAL", "KEYWORD_CLASS", "KEYWORD_PUBLIC",
  "KEYWORD_STATIC", "KEYWORD_VOID", "KEYWORD_MAIN", "KEYWORD_EXTENDS",
  "KEYWORD_RETURN", "KEYWORD_IF", "KEYWORD_ELSE", "KEYWORD_WHILE",
  "KEYWORD_PRINT", "KEYWORD_NEW", "KEYWORD_THIS", "PARENTHESIS_OPEN",
  "PARENTHESIS_CLOSE", "BRACKET_OPEN", "BRACKET_CLOSE",
  "CURLY_BRACKET_OPEN", "CURLY_BRACKET_CLOSE", "OP_AFFECT", "OP_ADD",
  "OP_SUBSTRACT", "OP_MULTIPLY", "OP_NOT", "LOG_AND", "LOG_OR", "LOG_LESS",
  "LOG_EQLESS", "LOG_MORE", "LOG_EQMORE", "LOG_EQ", "LOG_DIF",
  "SEMI_COLON", "DOT", "COMMA", "$accept", "Program", "MainClass",
  "NestedClassDeclaration", "MainClassDeclaration", "$@1", "$@2",
  "MainClassBody", "$@3", "ClassDeclaration", "$@4", "SimpleClassHeader",
  "MultipleVariablesDeclaration", "SimpleVariableDeclaration", "$@5",
  "InlineVariables", "$@6", "Variable", "MultipleMethodsDeclaration",
  "MethodDeclaration", "$@7", "$@8", "$@9", "$@10", "Type",
  "MultipleStatements", "Literal", "Statement", "$@11", "$@12", "$@13",
  "$@14", "$@15", "Expression", "$@16", "$@17", "CompositeExpression",
  "$@18", "$@19", "MethodCall", "MultipleExpressions", "$@20",
  "Arithmetic_Operator", "Logical_Operator", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-115)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-81)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
       5,    10,    12,    20,     5,    -2,     1,  -115,    35,  -115,
    -115,     5,    -4,   153,  -115,    29,  -115,  -115,    36,    28,
      14,    19,  -115,  -115,  -115,  -115,  -115,    22,    23,    26,
     153,  -115,    21,    48,    27,   -13,   153,    42,  -115,    45,
      28,  -115,    37,    31,   108,   108,   108,    30,    38,    39,
    -115,  -115,  -115,  -115,  -115,  -115,   108,  -115,    52,    44,
      28,    50,    45,  -115,   153,  -115,  -115,   151,  -115,   151,
      32,   151,   108,   108,    49,    55,    57,  -115,  -115,   108,
      45,    40,    58,    28,  -115,  -115,  -115,  -115,   151,   108,
    -115,  -115,  -115,  -115,  -115,  -115,  -115,  -115,    81,  -115,
      63,   108,   108,   151,  -115,    64,    68,  -115,    71,   151,
    -115,    65,    54,    61,    69,  -115,  -115,    45,    82,  -115,
      80,  -115,    67,   151,   151,  -115,   101,   108,   151,  -115,
     153,   153,  -115,  -115,  -115,    89,    85,    28,   151,  -115,
      90,    95,  -115,  -115,   151,    96,   100,  -115,  -115,    93,
     121,  -115,   104,   109,  -115,   151,    88,  -115,  -115,   151,
     151,   118,  -115,   112,  -115,    94,  -115,   108,   151,  -115,
    -115,  -115,   113,   115,    28,  -115,  -115,   153,   137,   153,
    -115,  -115,  -115,   127,   120,  -115,  -115,   108,   105,   123,
    -115,    45,   124,  -115
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,     0,     5,     0,     0,    14,     0,     1,
       2,     5,     0,    38,     3,     0,    15,     4,     0,    17,
      42,    34,    33,    35,    40,    41,    39,     0,     0,     0,
      38,    44,    18,     0,     0,     0,    37,     0,    11,    26,
      17,    18,     0,     0,     0,     0,     0,     0,     0,     0,
      24,     9,    81,    83,    82,    45,     0,    36,     0,     0,
       0,     0,    26,    16,     0,    32,    59,    76,    57,    76,
       0,    76,     0,     0,     0,     0,     0,    43,    19,     0,
      26,     0,     0,    17,    27,    13,    25,    49,    76,     0,
      84,    86,    85,    87,    88,    89,    90,    91,     0,    61,
       0,     0,     0,    76,    62,     0,     0,    63,     0,    76,
      51,     0,     0,     0,     0,    50,     6,    26,     0,    60,
       0,    77,     0,    76,    76,    58,     0,     0,    76,    67,
      38,    38,    56,    47,    10,     0,     0,    23,    76,    74,
      78,     0,    69,    70,    76,     0,     0,    68,    52,     0,
       0,    12,     0,    20,    71,    76,     0,    72,    65,    76,
      76,     0,    55,     0,    28,     0,    75,     0,    76,    66,
      64,    53,     0,     0,    23,    79,    73,    38,     0,    38,
      21,    54,     7,     0,     0,    29,     8,     0,     0,     0,
      30,    26,     0,    31
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -115,  -115,  -115,   135,  -115,  -115,  -115,  -115,  -115,  -115,
    -115,   149,   -31,   -14,  -115,   -21,  -115,   -19,   -56,  -115,
    -115,  -115,  -115,  -115,  -115,   -29,  -115,    99,  -115,  -115,
    -115,  -115,  -115,   -42,  -115,  -115,   -61,  -115,  -115,  -115,
    -114,  -115,   119,  -115
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     3,     4,    10,     5,   135,   184,    14,    80,    11,
      59,    12,    39,    31,    48,   152,   165,    32,    61,    62,
     118,   173,   187,   191,    33,    34,    35,    36,    49,    42,
     130,   161,   177,   140,   103,    88,    99,   168,   155,   100,
     141,   156,   101,   102
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      41,    47,    74,    75,    76,    40,    86,    57,   104,    63,
     107,    18,   145,     7,    81,     1,     2,    52,    53,    54,
       9,    41,     8,    19,   114,    13,    40,   119,    15,    55,
     108,   109,    21,    22,    23,   105,   106,   113,    16,    38,
      37,    84,   125,   -48,    43,    44,    45,   120,   129,    46,
     -46,    50,   117,   175,    58,    51,    60,    65,    77,   123,
     124,   136,   142,   143,    41,    82,    64,   147,    79,    40,
      66,    83,   116,   110,    67,    68,    69,   154,    85,   111,
      78,   112,   115,   158,   121,   146,   122,   126,    70,    71,
      72,   139,   131,   127,   166,   128,   132,   134,   169,   170,
      73,   148,   149,   133,    66,   137,   138,   176,    67,    68,
      69,    66,   150,   151,   -80,    67,    68,    69,   153,   157,
     159,   162,    70,    71,    72,   144,   160,   163,   164,    70,
      71,    72,   167,   -22,    73,   192,   171,   172,   174,   178,
     182,    73,   179,   185,   186,   188,    17,   189,   181,     6,
     183,   190,   193,   180,    56,   153,    20,    21,    22,    23,
      24,    25,    26,    87,     0,     0,     0,     0,     0,     0,
      27,     0,    28,    29,     0,     0,    89,     0,     0,     0,
      30,    52,    53,    54,     0,    90,    91,    92,    93,    94,
      95,    96,    97,     0,    98
};

static const yytype_int16 yycheck[] =
{
      19,    30,    44,    45,    46,    19,    62,    36,    69,    40,
      71,    15,   126,     3,    56,    10,    11,    30,    31,    32,
       0,    40,    10,    27,    80,    27,    40,    88,    27,    42,
      72,    73,     4,     5,     6,     3,     4,    79,     3,     3,
      11,    60,   103,    29,    25,    23,    23,    89,   109,    23,
      29,     3,    83,   167,    12,    28,    11,    26,    28,   101,
     102,   117,   123,   124,    83,    13,    29,   128,    29,    83,
       3,    27,    14,    24,     7,     8,     9,   138,    28,    24,
      42,    24,    42,   144,     3,   127,    23,    23,    21,    22,
      23,    24,    27,    25,   155,    24,    42,    28,   159,   160,
      33,   130,   131,    42,     3,    23,    26,   168,     7,     8,
       9,     3,    23,    28,    24,     7,     8,     9,   137,    24,
      24,    28,    21,    22,    23,    24,    26,     6,    24,    21,
      22,    23,    44,    24,    33,   191,    18,    25,    44,    26,
       3,    33,    27,    16,    24,   187,    11,    42,   177,     0,
     179,    28,    28,   174,    35,   174,     3,     4,     5,     6,
       7,     8,     9,    64,    -1,    -1,    -1,    -1,    -1,    -1,
      17,    -1,    19,    20,    -1,    -1,    25,    -1,    -1,    -1,
      27,    30,    31,    32,    -1,    34,    35,    36,    37,    38,
      39,    40,    41,    -1,    43
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,    10,    11,    46,    47,    49,    56,     3,    10,     0,
      48,    54,    56,    27,    52,    27,     3,    48,    15,    27,
       3,     4,     5,     6,     7,     8,     9,    17,    19,    20,
      27,    58,    62,    69,    70,    71,    72,    11,     3,    57,
      58,    62,    74,    25,    23,    23,    23,    70,    59,    73,
       3,    28,    30,    31,    32,    42,    87,    70,    12,    55,
      11,    63,    64,    57,    29,    26,     3,     7,     8,     9,
      21,    22,    23,    33,    78,    78,    78,    28,    42,    29,
      53,    78,    13,    27,    62,    28,    63,    72,    80,    25,
      34,    35,    36,    37,    38,    39,    40,    41,    43,    81,
      84,    87,    88,    79,    81,     3,     4,    81,    78,    78,
      24,    24,    24,    78,    63,    42,    14,    57,    65,    81,
      78,     3,    23,    78,    78,    81,    23,    25,    24,    81,
      75,    27,    42,    42,    28,    50,    63,    23,    26,    24,
      78,    85,    81,    81,    24,    85,    78,    81,    70,    70,
      23,    28,    60,    62,    81,    83,    86,    24,    81,    24,
      26,    76,    28,     6,    24,    61,    81,    44,    82,    81,
      81,    18,    25,    66,    44,    85,    81,    77,    26,    27,
      60,    70,     3,    70,    51,    16,    24,    67,    78,    42,
      28,    68,    63,    28
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    45,    46,    47,    48,    48,    50,    51,    49,    53,
      52,    55,    54,    54,    56,    56,    57,    57,    59,    58,
      61,    60,    60,    60,    62,    63,    63,    65,    66,    67,
      68,    64,    69,    69,    69,    69,    70,    70,    70,    71,
      71,    71,    71,    72,    72,    72,    73,    72,    74,    72,
      72,    75,    76,    77,    72,    72,    72,    79,    78,    80,
      78,    78,    78,    78,    78,    78,    78,    78,    78,    81,
      81,    81,    82,    81,    83,    81,    81,    84,    86,    85,
      85,    87,    87,    87,    88,    88,    88,    88,    88,    88,
      88,    88
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     2,     2,     0,     0,     0,    14,     0,
       6,     0,     8,     5,     2,     3,     2,     0,     0,     3,
       0,     4,     1,     0,     2,     2,     0,     0,     0,     0,
       0,    17,     3,     1,     1,     1,     2,     1,     0,     1,
       1,     1,     1,     3,     1,     2,     0,     5,     0,     4,
       4,     0,     0,     0,    10,     7,     5,     0,     3,     0,
       3,     2,     2,     2,     6,     5,     6,     3,     4,     3,
       3,     4,     0,     6,     0,     5,     0,     2,     0,     4,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep)
{
  YY_USE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 6: /* $@1: %empty  */
#line 88 "javaComp.y"
                                                                                                                                 {checkFuncID("main");line = create_code_line("ENTREE",-1,"");insert_code_line(line,current_index);current_index++;}
#line 1579 "javaComp.tab.c"
    break;

  case 7: /* $@2: %empty  */
#line 88 "javaComp.y"
                                                                                                                                                                                                                                                                                                                       {checkVarID(name);}
#line 1585 "javaComp.tab.c"
    break;

  case 8: /* MainClassDeclaration: SimpleClassHeader CURLY_BRACKET_OPEN KEYWORD_PUBLIC KEYWORD_STATIC KEYWORD_VOID KEYWORD_MAIN $@1 PARENTHESIS_OPEN TYPE_STRING BRACKET_OPEN BRACKET_CLOSE IDENTIFIER $@2 PARENTHESIS_CLOSE  */
#line 88 "javaComp.y"
                                                                                                                                                                                                                                                                                                                                                            {funcDecEnd();}
#line 1591 "javaComp.tab.c"
    break;

  case 9: /* $@3: %empty  */
#line 90 "javaComp.y"
                                                                                         {endFunction();line = create_code_line("SORTIE",-1,"");insert_code_line(line,current_index);current_index++;}
#line 1597 "javaComp.tab.c"
    break;

  case 10: /* MainClassBody: CURLY_BRACKET_OPEN MultipleStatements CURLY_BRACKET_CLOSE $@3 MultipleMethodsDeclaration CURLY_BRACKET_CLOSE  */
#line 90 "javaComp.y"
                                                                                                                                                                                                                                                     {endClass();}
#line 1603 "javaComp.tab.c"
    break;

  case 11: /* $@4: %empty  */
#line 92 "javaComp.y"
                                                                     {checkClassID(name);}
#line 1609 "javaComp.tab.c"
    break;

  case 12: /* ClassDeclaration: SimpleClassHeader KEYWORD_EXTENDS IDENTIFIER $@4 CURLY_BRACKET_OPEN MultipleVariablesDeclaration MultipleMethodsDeclaration CURLY_BRACKET_CLOSE  */
#line 92 "javaComp.y"
                                                                                                                                                                                         {endClass();}
#line 1615 "javaComp.tab.c"
    break;

  case 13: /* ClassDeclaration: SimpleClassHeader CURLY_BRACKET_OPEN MultipleVariablesDeclaration MultipleMethodsDeclaration CURLY_BRACKET_CLOSE  */
#line 93 "javaComp.y"
                                                                                                                                         {endClass();}
#line 1621 "javaComp.tab.c"
    break;

  case 14: /* SimpleClassHeader: KEYWORD_CLASS IDENTIFIER  */
#line 95 "javaComp.y"
                                                          {checkClassID(name);}
#line 1627 "javaComp.tab.c"
    break;

  case 15: /* SimpleClassHeader: KEYWORD_PUBLIC KEYWORD_CLASS IDENTIFIER  */
#line 96 "javaComp.y"
                                                                   {checkClassID(name);}
#line 1633 "javaComp.tab.c"
    break;

  case 18: /* $@5: %empty  */
#line 101 "javaComp.y"
                                             {checkVarID(name);initVar(name,yylineno);}
#line 1639 "javaComp.tab.c"
    break;

  case 20: /* $@6: %empty  */
#line 103 "javaComp.y"
                                        {checkVarID(name);}
#line 1645 "javaComp.tab.c"
    break;

  case 22: /* InlineVariables: Variable  */
#line 104 "javaComp.y"
                                  {checkVarID(name);}
#line 1651 "javaComp.tab.c"
    break;

  case 27: /* $@7: %empty  */
#line 112 "javaComp.y"
                                                 {checkFuncID(name);strcpy(current_id,name);}
#line 1657 "javaComp.tab.c"
    break;

  case 28: /* $@8: %empty  */
#line 112 "javaComp.y"
                                                                                                                                                 {funcDecEnd();}
#line 1663 "javaComp.tab.c"
    break;

  case 29: /* $@9: %empty  */
#line 112 "javaComp.y"
                                                                                                                                                                                                                       {line = create_code_line("RETURN",getAddress(name,local_table),current_id);insert_code_line(line,current_index);current_index++; }
#line 1669 "javaComp.tab.c"
    break;

  case 30: /* $@10: %empty  */
#line 112 "javaComp.y"
                                                                                                                                                                                                                                                                                                                                                                                                    {endFunction();line = create_code_line("SORTIE",-1,"");insert_code_line(line,current_index);current_index++;}
#line 1675 "javaComp.tab.c"
    break;

  case 31: /* MethodDeclaration: KEYWORD_PUBLIC Variable $@7 PARENTHESIS_OPEN InlineVariables PARENTHESIS_CLOSE $@8 CURLY_BRACKET_OPEN MultipleStatements KEYWORD_RETURN $@9 Expression SEMI_COLON CURLY_BRACKET_CLOSE $@10 MultipleMethodsDeclaration CURLY_BRACKET_CLOSE  */
#line 112 "javaComp.y"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                {endClass();}
#line 1681 "javaComp.tab.c"
    break;

  case 41: /* Literal: INTEGER_LITERAL  */
#line 126 "javaComp.y"
                                        {line = create_code_line("LDC",value,"");insert_code_line(line,current_index);current_index++;}
#line 1687 "javaComp.tab.c"
    break;

  case 42: /* Literal: IDENTIFIER  */
#line 127 "javaComp.y"
                                    {checkID(name);line = create_code_line("LDV",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++; }
#line 1693 "javaComp.tab.c"
    break;

  case 46: /* $@11: %empty  */
#line 132 "javaComp.y"
                                  {checkVarID(name);initVar(name,yylineno);strcpy(current_id,name);}
#line 1699 "javaComp.tab.c"
    break;

  case 47: /* Statement: Variable $@11 OP_AFFECT Expression SEMI_COLON  */
#line 132 "javaComp.y"
                                                                                                                                    {line=create_code_line("STORE",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++;}
#line 1705 "javaComp.tab.c"
    break;

  case 48: /* $@12: %empty  */
#line 133 "javaComp.y"
                                    {checkID(name);strcpy(current_id,name);}
#line 1711 "javaComp.tab.c"
    break;

  case 49: /* Statement: IDENTIFIER $@12 OP_AFFECT Statement  */
#line 133 "javaComp.y"
                                                                                                    {line=create_code_line("STORE",getAddress(current_id,local_table),"");insert_code_line(line,current_index);current_index++;}
#line 1717 "javaComp.tab.c"
    break;

  case 50: /* Statement: Literal Arithmetic_Operator Expression SEMI_COLON  */
#line 134 "javaComp.y"
                                                                           {line = create_code_line(operator,-1,"");insert_code_line(line,current_index);current_index++;}
#line 1723 "javaComp.tab.c"
    break;

  case 51: /* $@13: %empty  */
#line 135 "javaComp.y"
                                                                                   {line = create_code_line("SIFAUX",-1,"");insert_code_line(line,current_index);if_index = current_index++;}
#line 1729 "javaComp.tab.c"
    break;

  case 52: /* $@14: %empty  */
#line 137 "javaComp.y"
                            {line = create_code_line("SAUT",-1,"");insert_code_line(line,current_index);saut_index = current_index++;}
#line 1735 "javaComp.tab.c"
    break;

  case 53: /* $@15: %empty  */
#line 138 "javaComp.y"
                                         {code[if_index].operand = current_index;}
#line 1741 "javaComp.tab.c"
    break;

  case 54: /* Statement: KEYWORD_IF PARENTHESIS_OPEN Expression PARENTHESIS_CLOSE $@13 MultipleStatements $@14 KEYWORD_ELSE $@15 MultipleStatements  */
#line 140 "javaComp.y"
                            {code[saut_index].operand = current_index;}
#line 1747 "javaComp.tab.c"
    break;

  case 57: /* $@16: %empty  */
#line 145 "javaComp.y"
                                         {line = create_code_line("LDC",value,"");insert_code_line(line,current_index);current_index++;}
#line 1753 "javaComp.tab.c"
    break;

  case 59: /* $@17: %empty  */
#line 146 "javaComp.y"
                                    {checkID(name);line = create_code_line("LDV",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++; }
#line 1759 "javaComp.tab.c"
    break;

  case 70: /* CompositeExpression: Logical_Operator Expression CompositeExpression  */
#line 157 "javaComp.y"
                                                                           {line = create_code_line(operator,-1,"");insert_code_line(line,current_index);current_index++;}
#line 1765 "javaComp.tab.c"
    break;

  case 72: /* $@18: %empty  */
#line 159 "javaComp.y"
                                                                                          {funcCallEnd();}
#line 1771 "javaComp.tab.c"
    break;

  case 74: /* $@19: %empty  */
#line 160 "javaComp.y"
                                                                       {g_nbParam = 0;funcCallEnd();}
#line 1777 "javaComp.tab.c"
    break;

  case 78: /* $@20: %empty  */
#line 165 "javaComp.y"
                                           {g_nbParam++;}
#line 1783 "javaComp.tab.c"
    break;

  case 80: /* MultipleExpressions: Expression  */
#line 166 "javaComp.y"
                                    {g_nbParam++;}
#line 1789 "javaComp.tab.c"
    break;

  case 81: /* Arithmetic_Operator: OP_ADD  */
#line 168 "javaComp.y"
                                           {strcpy(operator,"ADD");}
#line 1795 "javaComp.tab.c"
    break;

  case 82: /* Arithmetic_Operator: OP_MULTIPLY  */
#line 169 "javaComp.y"
                                     {strcpy(operator,"MUL");}
#line 1801 "javaComp.tab.c"
    break;

  case 83: /* Arithmetic_Operator: OP_SUBSTRACT  */
#line 170 "javaComp.y"
                                      {strcpy(operator,"SUB");}
#line 1807 "javaComp.tab.c"
    break;

  case 84: /* Logical_Operator: LOG_AND  */
#line 172 "javaComp.y"
                                            {strcpy(operator,"AND");}
#line 1813 "javaComp.tab.c"
    break;

  case 85: /* Logical_Operator: LOG_LESS  */
#line 173 "javaComp.y"
                                  {strcpy(operator,"INF");}
#line 1819 "javaComp.tab.c"
    break;

  case 86: /* Logical_Operator: LOG_OR  */
#line 174 "javaComp.y"
                                {strcpy(operator,"OR");}
#line 1825 "javaComp.tab.c"
    break;

  case 87: /* Logical_Operator: LOG_EQLESS  */
#line 175 "javaComp.y"
                                    {strcpy(operator,"INFEQ");}
#line 1831 "javaComp.tab.c"
    break;

  case 88: /* Logical_Operator: LOG_MORE  */
#line 176 "javaComp.y"
                                  {strcpy(operator,"SUP");}
#line 1837 "javaComp.tab.c"
    break;

  case 89: /* Logical_Operator: LOG_EQMORE  */
#line 177 "javaComp.y"
                                    {strcpy(operator,"SUPEQ");}
#line 1843 "javaComp.tab.c"
    break;

  case 90: /* Logical_Operator: LOG_EQ  */
#line 178 "javaComp.y"
                                {strcpy(operator,"EQ");}
#line 1849 "javaComp.tab.c"
    break;

  case 91: /* Logical_Operator: LOG_DIF  */
#line 179 "javaComp.y"
                                 {strcpy(operator,"DIF");}
#line 1855 "javaComp.tab.c"
    break;


#line 1859 "javaComp.tab.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 182 "javaComp.y"



extern FILE *yyin;
int main(int argc, char **argv)
{
    yyin = fopen(argv[1], "r");
    beginSemantic();
    beginCodeGen();
    yyparse();
    endCodeGen();
    endSemantic();
    if(errorCount == 0) {
     fprintf(stdout,"File Compiled Successfully \n");
     if(warningCount > 0)
          fprintf(stdout,"Compiler terminated with %d warning(s)\n",warningCount);
    }
    else {
     fprintf(stderr,"Compiler terminated with %d error(s) and %d warning(s)",errorCount,warningCount);
    }
    return 1;
}
void beginSemantic()
{
	table = NULL;
	local_table = NULL;
	class_table = NULL;
	g_nbParam = 0;
	g_IfFunc = 0 ;
     g_IfFuncParameters = 0 ;
     g_IfClass = 0 ;
}
void endSemantic()
{
     fclose(yyin);
     destructSymbolsTable(local_table);
	destructSymbolsTable(table);
}
void beginCodeGen(){
     current_index = 0;
     create_code_array(1000);
}
void endCodeGen(){
     generate_code(current_index);
}
