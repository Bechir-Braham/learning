#include <stdio.h>
#include <stdlib.h>

typedef struct node{
    int val;
    struct node* suivant;
}node;
typedef node* liste;
liste push(liste l,int x){
    liste q=l;
    liste tmp=(liste)malloc(sizeof(node));
    tmp->suivant=NULL;
    tmp->val=x;
    if(!l) return tmp;
    while(l->suivant != NULL) l=l->suivant;
    l->suivant=tmp;
    return q;
}

void p(liste l){
    for(liste tmp=l;tmp!=NULL;tmp=tmp->suivant)
        printf("%d\t",tmp->val);
    return;
}




liste min_list(liste l){
    int min=l->val;
    liste addr=l;
    while(l!=NULL){
        if(l->val<min){
            min=l->val;
            addr=l;
        }
        l=l->suivant;
    }
    return addr;
}
void permute(liste l1,liste l2){
  int x=l1->val;
  l1->val=l2->val;
  l2->val=x;
}
liste tri(liste l){
    liste q=l;
    while(l!=NULL){
        liste tmp=min_list(l);
        permute(l,tmp);
        p(q);
        printf("\n");
        l=l->suivant;
    }
    return q;
}
int main(){
    liste list=NULL;
    list=push(list,19);
    push(list,99);
    push(list,2);
    list=push(list,1);
    list=push(list,3);
    list=push(list,8);
    list=push(list,79);
    list=push(list,10);
    p(list);
    printf("%d\n",min_list(list)->val);
    tri(list);
    p(list);

    return 0;
}