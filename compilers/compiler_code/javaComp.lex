%{	
 #include <stdio.h>	
 #include <stdlib.h>	
 #include "javaComp.tab.h"	                                                                         	
 #include <math.h>	 			
 #include<string.h>


 int errorCount = 0;
 int warningCount = 0;
 extern char name[];
 extern int value;
 void yyerror(char const *msg);
 void lexicalError(char const *msg);
 void semanticError(char const *msg);
 void semanticWarning(char *msg,int lineNumber);
%}

%option yylineno

DELIM     [ \t]
BL        {DELIM}+
SEMI_COLON ";"
DOT "."
COMMA ","
DIGIT   [0-9]
LETTER    [a-zA-Z]

INTEGER_LITERAL {DIGIT}+
BOOLEAN_LITERAL true|false
STRING_LITERAL \"([^"\n]|\"\")+\"
WRONG_STRING_LITERAL \"[^\n"]*

ACCEPTED_SPECIAL _|$

IDENTIFIER        ({LETTER}|{ACCEPTED_SPECIAL})({LETTER}|{DIGIT}|{ACCEPTED_SPECIAL})*
NONIDENTIFIER  ({DIGIT}({LETTER}|{DIGIT}|_)+)

OPENING_PARENTHESIS  (\()
CLOSING_PARENTHESIS  (\))

OPENING_CURLY_BRACKETS  "{"
CLOSING_CURLY_BRACKETS  "}"

OPENING_BRACKET  (\[)
CLOSING_BRACKET  (\])

COMMENT_BLOCK                  "/*"([^*]|\*+[^*/])*\*+"/"
COMMENT_LINE                   "//".*\n
ERROR_COMMENT                   \/\*([^(\*\/)]|\n)*

%%


{BL}                                  /* no actions */

"\n"	                            /* no actions */
"System.out.println"                        return KEYWORD_PRINT;
"new"                                       return KEYWORD_NEW;
"class"                               return KEYWORD_CLASS;
"public"                              return KEYWORD_PUBLIC;
"static"                    return KEYWORD_STATIC;
"void"                      return KEYWORD_VOID;
"main"                       return KEYWORD_MAIN;
"extends"                             return KEYWORD_EXTENDS;
"this"                                return KEYWORD_THIS;
"return"                              return KEYWORD_RETURN;

"if"                                  return KEYWORD_IF;

"else"                                return KEYWORD_ELSE;

"while"                               return KEYWORD_WHILE;

"int"                                 return TYPE_INT;

"boolean"                             return TYPE_BOOLEAN;

"String"                              return TYPE_STRING;


{INTEGER_LITERAL}                           {value = atoi(yytext)  ;return INTEGER_LITERAL;}

{BOOLEAN_LITERAL}                           return BOOLEAN_LITERAL;

{STRING_LITERAL}                            return STRING_LITERAL;

{IDENTIFIER} {strcpy(name,yytext);return IDENTIFIER;}

{SEMI_COLON} return SEMI_COLON;

{DOT} return DOT;

{COMMA} return COMMA;


{OPENING_PARENTHESIS}                     return PARENTHESIS_OPEN;

{CLOSING_PARENTHESIS}                    return PARENTHESIS_CLOSE;

{OPENING_BRACKET}                        return BRACKET_OPEN;

{CLOSING_BRACKET}                                   return BRACKET_CLOSE;

{OPENING_CURLY_BRACKETS}                            return CURLY_BRACKET_OPEN;

{CLOSING_CURLY_BRACKETS}                             return CURLY_BRACKET_CLOSE;

"="	                                 return OP_AFFECT;
"+"                                   return OP_ADD;
"-"                                   return OP_SUBSTRACT;
"*"                                   return OP_MULTIPLY;
"!"                                   return OP_NOT;
"||"                                  return LOG_OR;
"&&"                                  return LOG_AND;
"<"                                   return LOG_LESS;
"<="                                  return LOG_EQLESS;
">"                                   return LOG_MORE;
">="                                  return LOG_EQMORE;
"=="                                  return LOG_EQ;
"!="                                  return LOG_DIF;


{COMMENT_LINE}         		      /* no actions */

{COMMENT_BLOCK}         		      /* no actions */

{NONIDENTIFIER} {lexicalError("illegal identifier");}

{WRONG_STRING_LITERAL}                      {lexicalError("illegal string literal");}

{ERROR_COMMENT} {lexicalError("illegal comment");}


%%
void yyerror(char const *msg) {
     errorCount++;
	fprintf(stderr, "%s at line %d\n",msg,yylineno);
     exit(0);
}
int yywrap()
{
     return(1);
}
void lexicalError(const char* msg){
     char errorStr[200];
     sprintf(errorStr,"Lexical error :%s",msg);
     yyerror(errorStr);
}
void semanticError (const char *str){
    char errstr[200];
    sprintf(errstr,"Semantic error  : %s",str);
    yyerror(errstr);
}
void semanticWarning (char *nom,int lineNumber){
    warningCount++;
    fprintf(stdout,"Semantic warning : declared variable is not used : %s at line %d\n",nom,lineNumber);
}
