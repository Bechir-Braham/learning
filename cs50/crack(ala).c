#include <stdio.h>
#include <string.h>
void inc(char*, int, int*);

int main(int argc, char* argv[]){
    FILE *fp = NULL;
    char pwd[6] = "a";
    int i = 0;

    fp = fopen("combinations", "w");
    if(fp != NULL){
        while(strlen(pwd) < 6){
            fprintf(fp, "%s\n", pwd);
            inc(pwd, i, &i);
        }
        printf("Task done.\n");
        fclose(fp);
    } else {
        printf("Error while loading file\n");
    }
    return 0;
}

void inc(char* str, int idx, int *p){
    if(idx < 0){
        strcat(str, "a");
        (*p)++;
    } else if(str[idx] == 'z'){
        str[idx] = 'a';
        inc(str, idx - 1, p);
    } else {
        str[idx]++;
    }
}
