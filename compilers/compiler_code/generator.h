#ifndef GENERATOR
#define GENERATOR

typedef struct {
    char * op_name;
    int operand;
    char * fct_name;
}code_line;

void create_code_array(int size);
code_line create_code_line(char * op_name, int operand, char * fct_name);
void insert_code_line(code_line line, int index);
void generate_code(int size);
#endif