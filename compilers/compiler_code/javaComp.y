%{
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
%}

%token IDENTIFIER
%token TYPE_INT
%token TYPE_BOOLEAN
%token TYPE_STRING

%token BOOLEAN_LITERAL
%token INTEGER_LITERAL
%token STRING_LITERAL

%token KEYWORD_CLASS
%token KEYWORD_PUBLIC
%token KEYWORD_STATIC
%token KEYWORD_VOID
%token KEYWORD_MAIN
%token KEYWORD_EXTENDS
%token KEYWORD_RETURN
%token KEYWORD_IF
%token KEYWORD_ELSE
%token KEYWORD_WHILE
%token KEYWORD_PRINT
%token KEYWORD_NEW
%token KEYWORD_THIS

%token PARENTHESIS_OPEN
%token PARENTHESIS_CLOSE
%token BRACKET_OPEN
%token BRACKET_CLOSE
%token CURLY_BRACKET_OPEN
%token CURLY_BRACKET_CLOSE

%token OP_AFFECT
%token OP_ADD
%token OP_SUBSTRACT
%token OP_MULTIPLY
%token OP_NOT

%token LOG_AND
%token LOG_OR
%token LOG_LESS
%token LOG_EQLESS
%token LOG_MORE
%token LOG_EQMORE
%token LOG_EQ
%token LOG_DIF

%token SEMI_COLON
%token DOT
%token COMMA

%define parse.error verbose
%start Program


%%

Program		           : MainClass NestedClassDeclaration
                       ;
MainClass              : MainClassDeclaration MainClassBody
                       ;
NestedClassDeclaration	   : ClassDeclaration NestedClassDeclaration
                       |
                       ;
MainClassDeclaration               : SimpleClassHeader CURLY_BRACKET_OPEN KEYWORD_PUBLIC KEYWORD_STATIC KEYWORD_VOID KEYWORD_MAIN{checkFuncID("main");line = create_code_line("ENTREE",-1,"");insert_code_line(line,current_index);current_index++;} PARENTHESIS_OPEN TYPE_STRING BRACKET_OPEN BRACKET_CLOSE IDENTIFIER{checkVarID(name);} PARENTHESIS_CLOSE{funcDecEnd();}
                       ;
MainClassBody               :  CURLY_BRACKET_OPEN MultipleStatements  CURLY_BRACKET_CLOSE{endFunction();line = create_code_line("SORTIE",-1,"");insert_code_line(line,current_index);current_index++;} MultipleMethodsDeclaration CURLY_BRACKET_CLOSE{endClass();}
                       ;
ClassDeclaration       : SimpleClassHeader KEYWORD_EXTENDS IDENTIFIER{checkClassID(name);} CURLY_BRACKET_OPEN MultipleVariablesDeclaration MultipleMethodsDeclaration CURLY_BRACKET_CLOSE{endClass();} 
                       | SimpleClassHeader CURLY_BRACKET_OPEN MultipleVariablesDeclaration MultipleMethodsDeclaration CURLY_BRACKET_CLOSE{endClass();} 
                       ;
SimpleClassHeader              : KEYWORD_CLASS IDENTIFIER {checkClassID(name);}
                         | KEYWORD_PUBLIC KEYWORD_CLASS IDENTIFIER {checkClassID(name);}
                       ;
MultipleVariablesDeclaration        : SimpleVariableDeclaration MultipleVariablesDeclaration
                       |
                       ;
SimpleVariableDeclaration         : Variable {checkVarID(name);initVar(name,yylineno);} SEMI_COLON
                       ;
InlineVariables              : Variable {checkVarID(name);} COMMA InlineVariables
                       | Variable {checkVarID(name);}
                       |
                       ;
Variable               : Type IDENTIFIER 
                       ;
MultipleMethodsDeclaration     : MethodDeclaration MultipleMethodsDeclaration
                       |
                       ;
MethodDeclaration      : KEYWORD_PUBLIC Variable {checkFuncID(name);strcpy(current_id,name);} PARENTHESIS_OPEN InlineVariables PARENTHESIS_CLOSE {funcDecEnd();}  CURLY_BRACKET_OPEN MultipleStatements  KEYWORD_RETURN{line = create_code_line("RETURN",getAddress(name,local_table),current_id);insert_code_line(line,current_index);current_index++; } Expression SEMI_COLON CURLY_BRACKET_CLOSE {endFunction();line = create_code_line("SORTIE",-1,"");insert_code_line(line,current_index);current_index++;} MultipleMethodsDeclaration CURLY_BRACKET_CLOSE{endClass();}
                       ;

Type                   : TYPE_INT BRACKET_OPEN BRACKET_CLOSE 
                       | TYPE_BOOLEAN 
                       | TYPE_INT  
                       | TYPE_STRING 
                       ;
MultipleStatements             : Statement MultipleStatements
                       | Statement
                       |
                       ;
Literal                : STRING_LITERAL
                       | BOOLEAN_LITERAL
                       | INTEGER_LITERAL{line = create_code_line("LDC",value,"");insert_code_line(line,current_index);current_index++;}
                       | IDENTIFIER {checkID(name);line = create_code_line("LDV",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++; }
                       ;
Statement              : CURLY_BRACKET_OPEN MultipleStatements CURLY_BRACKET_CLOSE
                       | SimpleVariableDeclaration
                       | Literal SEMI_COLON
                       | Variable {checkVarID(name);initVar(name,yylineno);strcpy(current_id,name);} OP_AFFECT Expression SEMI_COLON{line=create_code_line("STORE",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++;}
                       | IDENTIFIER {checkID(name);strcpy(current_id,name);}   OP_AFFECT  Statement {line=create_code_line("STORE",getAddress(current_id,local_table),"");insert_code_line(line,current_index);current_index++;} 
                       | Literal Arithmetic_Operator Expression SEMI_COLON {line = create_code_line(operator,-1,"");insert_code_line(line,current_index);current_index++;}
                       | KEYWORD_IF  PARENTHESIS_OPEN Expression PARENTHESIS_CLOSE {line = create_code_line("SIFAUX",-1,"");insert_code_line(line,current_index);if_index = current_index++;}  
                            MultipleStatements
                            {line = create_code_line("SAUT",-1,"");insert_code_line(line,current_index);saut_index = current_index++;}
                            KEYWORD_ELSE {code[if_index].operand = current_index;}
                            MultipleStatements 
                            {code[saut_index].operand = current_index;}
                       | KEYWORD_WHILE  PARENTHESIS_OPEN Expression PARENTHESIS_CLOSE 
                            CURLY_BRACKET_OPEN MultipleStatements CURLY_BRACKET_CLOSE
                       | KEYWORD_PRINT PARENTHESIS_OPEN Expression PARENTHESIS_CLOSE SEMI_COLON
                       ;
Expression             : INTEGER_LITERAL {line = create_code_line("LDC",value,"");insert_code_line(line,current_index);current_index++;} CompositeExpression
                       | IDENTIFIER {checkID(name);line = create_code_line("LDV",getAddress(name,local_table),"");insert_code_line(line,current_index);current_index++; }  CompositeExpression
                       | BOOLEAN_LITERAL CompositeExpression
                       | STRING_LITERAL CompositeExpression
                       | KEYWORD_THIS CompositeExpression
                       | KEYWORD_NEW TYPE_INT BRACKET_OPEN Expression BRACKET_CLOSE CompositeExpression
                       | KEYWORD_NEW IDENTIFIER PARENTHESIS_OPEN PARENTHESIS_CLOSE CompositeExpression
                       | KEYWORD_NEW IDENTIFIER PARENTHESIS_OPEN MultipleExpressions PARENTHESIS_CLOSE CompositeExpression
                       | OP_NOT Expression CompositeExpression
                       | PARENTHESIS_OPEN Expression PARENTHESIS_CLOSE CompositeExpression
                       ;
CompositeExpression         : Arithmetic_Operator Expression CompositeExpression
                       | Logical_Operator  Expression  CompositeExpression {line = create_code_line(operator,-1,"");insert_code_line(line,current_index);current_index++;}
                       | BRACKET_OPEN Expression BRACKET_CLOSE CompositeExpression
                       | MethodCall PARENTHESIS_OPEN MultipleExpressions PARENTHESIS_CLOSE{funcCallEnd();} CompositeExpression
                       | MethodCall PARENTHESIS_OPEN PARENTHESIS_CLOSE {g_nbParam = 0;funcCallEnd();}  CompositeExpression
                       |
                       ;
MethodCall             : DOT IDENTIFIER 
                       ;
MultipleExpressions            : Expression{g_nbParam++;} COMMA MultipleExpressions
                       | Expression {g_nbParam++;}
                       ;
Arithmetic_Operator               : OP_ADD {strcpy(operator,"ADD");}
                       | OP_MULTIPLY {strcpy(operator,"MUL");}
                       | OP_SUBSTRACT {strcpy(operator,"SUB");}
                       ;
Logical_Operator                  : LOG_AND {strcpy(operator,"AND");}
                       | LOG_LESS {strcpy(operator,"INF");}
                       | LOG_OR {strcpy(operator,"OR");}
                       | LOG_EQLESS {strcpy(operator,"INFEQ");}
                       | LOG_MORE {strcpy(operator,"SUP");}
                       | LOG_EQMORE {strcpy(operator,"SUPEQ");}
                       | LOG_EQ {strcpy(operator,"EQ");}
                       | LOG_DIF {strcpy(operator,"DIF");}
                       ;

%%


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
