#include <stdio.h>
#include<ctype.h>
#include<string.h>

int main(int argc, char *argv[])
{
    char s[50];
    if(argc!=2 && !isdigit(*argv[1]))
    {
        printf("error");
        return 1;
    }
    
    printf("type your string :  ");
    scanf("%[^\n]",s);
 
    for (int i=0;i<strlen(s);++i)
    {
        if (s[i]>='A' && s[i]<='Z')
        {
             s[i]= ((s[i]-'A' + *argv[1]-'0')%26) +'A';   
        }
       if (s[i]>='a' && s[i]<='z')
        {
             s[i]= ((s[i]-'a' + *argv[1]-'0')%26) +'a';   
        }
   printf("%c",s[i]);
    }
    printf("\n\nyour new string is : %s\n\n",s);
    
return 0;
}
