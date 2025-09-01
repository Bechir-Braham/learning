#include "generator.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
code_line * code;
void  create_code_array(int size) {
    code  = malloc(sizeof(code_line) * size);
}
code_line create_code_line(char * op_name, int operand, char * fct_name) {
    code_line code_line;
    code_line.op_name = malloc(sizeof(char) * strlen(op_name));
    code_line.fct_name = malloc(sizeof(char) * strlen(fct_name));
    strcpy(code_line.op_name, op_name);
    code_line.operand = operand;
    strcpy(code_line.fct_name, fct_name);
    return code_line;
}
void generate_code(int size) {
    for (int i = 0; i < size; i++) {
        printf("%s", code[i].op_name);
        if (code[i].operand != -1) {
            printf(" %d", code[i].operand);
        }
        printf("\n");
    }
}
void insert_code_line(code_line line, int index) {
    code[index] = line;
}