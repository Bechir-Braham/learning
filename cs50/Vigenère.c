#include <stdio.h>
#include<string.h>
#include<ctype.h>

int main(int argc,char *argv[])
{
    char s[100];
    if (argc != 2)
    {
        printf("keyword error(argc)");
        return 1;
    }

    for (int i=0;i<=strlen(argv[1]-1);i++)
    {
        if(!isalpha(argv[1][i]))
        {
            printf("keyword error(not alpha)");
            return 1;
        }
    }

    printf("enter your plaintext: ");
    scanf("%[^\n]",s);
    unsigned long c=strlen(argv[1]);
    unsigned long k=strlen(s)-1;
    for (int i =0,n=0;i<=k;++i)
    { 
        if(isalpha(s[i]))
        {
            printf("%d\n",argv[1][n%c]);
            s[i]=(s[i]-'a'+argv[1][n%c]-'a')%26+'a';
            ++n;
        }
    }
    printf("ciphertext is: %s",s);
    return 0;
}
