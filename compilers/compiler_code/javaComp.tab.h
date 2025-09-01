/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_JAVACOMP_TAB_H_INCLUDED
# define YY_YY_JAVACOMP_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENTIFIER = 258,              /* IDENTIFIER  */
    TYPE_INT = 259,                /* TYPE_INT  */
    TYPE_BOOLEAN = 260,            /* TYPE_BOOLEAN  */
    TYPE_STRING = 261,             /* TYPE_STRING  */
    BOOLEAN_LITERAL = 262,         /* BOOLEAN_LITERAL  */
    INTEGER_LITERAL = 263,         /* INTEGER_LITERAL  */
    STRING_LITERAL = 264,          /* STRING_LITERAL  */
    KEYWORD_CLASS = 265,           /* KEYWORD_CLASS  */
    KEYWORD_PUBLIC = 266,          /* KEYWORD_PUBLIC  */
    KEYWORD_STATIC = 267,          /* KEYWORD_STATIC  */
    KEYWORD_VOID = 268,            /* KEYWORD_VOID  */
    KEYWORD_MAIN = 269,            /* KEYWORD_MAIN  */
    KEYWORD_EXTENDS = 270,         /* KEYWORD_EXTENDS  */
    KEYWORD_RETURN = 271,          /* KEYWORD_RETURN  */
    KEYWORD_IF = 272,              /* KEYWORD_IF  */
    KEYWORD_ELSE = 273,            /* KEYWORD_ELSE  */
    KEYWORD_WHILE = 274,           /* KEYWORD_WHILE  */
    KEYWORD_PRINT = 275,           /* KEYWORD_PRINT  */
    KEYWORD_NEW = 276,             /* KEYWORD_NEW  */
    KEYWORD_THIS = 277,            /* KEYWORD_THIS  */
    PARENTHESIS_OPEN = 278,        /* PARENTHESIS_OPEN  */
    PARENTHESIS_CLOSE = 279,       /* PARENTHESIS_CLOSE  */
    BRACKET_OPEN = 280,            /* BRACKET_OPEN  */
    BRACKET_CLOSE = 281,           /* BRACKET_CLOSE  */
    CURLY_BRACKET_OPEN = 282,      /* CURLY_BRACKET_OPEN  */
    CURLY_BRACKET_CLOSE = 283,     /* CURLY_BRACKET_CLOSE  */
    OP_AFFECT = 284,               /* OP_AFFECT  */
    OP_ADD = 285,                  /* OP_ADD  */
    OP_SUBSTRACT = 286,            /* OP_SUBSTRACT  */
    OP_MULTIPLY = 287,             /* OP_MULTIPLY  */
    OP_NOT = 288,                  /* OP_NOT  */
    LOG_AND = 289,                 /* LOG_AND  */
    LOG_OR = 290,                  /* LOG_OR  */
    LOG_LESS = 291,                /* LOG_LESS  */
    LOG_EQLESS = 292,              /* LOG_EQLESS  */
    LOG_MORE = 293,                /* LOG_MORE  */
    LOG_EQMORE = 294,              /* LOG_EQMORE  */
    LOG_EQ = 295,                  /* LOG_EQ  */
    LOG_DIF = 296,                 /* LOG_DIF  */
    SEMI_COLON = 297,              /* SEMI_COLON  */
    DOT = 298,                     /* DOT  */
    COMMA = 299                    /* COMMA  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


int yyparse (void);


#endif /* !YY_YY_JAVACOMP_TAB_H_INCLUDED  */
