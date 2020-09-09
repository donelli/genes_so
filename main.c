#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

typedef struct nodo {
  char * nomeOrganismo;
  char * nomeGene;
  char * linha;
  struct nodo* proximo;
} T_NODO;

typedef struct head_lista {
  T_NODO * head;
} T_HEAD_LISTA;

typedef struct args {
  int inicio;
  int incremento;
  char * arquivo;
  T_HEAD_LISTA * head_lista;
} T_THREAD_ARGS;


void sortedInsert(T_NODO** head_ref, T_NODO* new_node) { 
  
  T_NODO * anterior = NULL;
  T_NODO * atual = *head_ref;
  int comp;
  
  while (NULL != atual && ( (comp = strcmp(new_node->nomeGene, atual->nomeGene)) > 0 ||
    (comp == 0 && strcmp(new_node->nomeOrganismo, atual->nomeOrganismo) > 0 ) ) ) {
    anterior = atual;
    atual = atual->proximo;
  }
  
  if(anterior == NULL){
    new_node->proximo = atual;
    *head_ref = new_node; 
  } else {
    anterior->proximo = new_node;
    new_node->proximo = atual;
  }
  
}

void moveNode(T_NODO ** destRef, T_NODO** sourceRef) { 
  
  T_NODO* newNode = *sourceRef; 
  
  *sourceRef = newNode->proximo; 
  newNode->proximo = *destRef; 
  *destRef = newNode; 
}

T_NODO * mergeLinkedLists(T_THREAD_ARGS args[], int numThreads) {
  
  T_NODO * head = NULL;
  T_NODO * tail = NULL;
  T_NODO * aux = NULL;
  int i, indice;
  
  while (true) {
    
    aux = NULL;
    indice = -1;
    
    for (i = 0; i < numThreads; i++) {
      
      if (args[i].head_lista->head == NULL) {
        continue;
      }
      
      if (aux == NULL) {
        aux = args[i].head_lista->head;
        indice = i;
      } else {
        
        if (strcmp(args[i].head_lista->head->nomeGene, aux->nomeGene) < 0) {
          aux = args[i].head_lista->head;
          indice = i;
        } else if (strcmp(args[i].head_lista->head->nomeOrganismo, aux->nomeOrganismo) < 0) {
          aux = args[i].head_lista->head;
          indice = i;
        }
        
      }
      
    }
    
    if (aux == NULL) {
      break;
    }
    
    if (head == NULL) {
      head = aux;
      tail = aux;
    } else {
      tail->proximo = aux;
      tail = aux;
    }
    
    args[indice].head_lista->head = args[indice].head_lista->head->proximo;
    
  }
  
  return head;
  
}

void * processaArquivo(void *arg) {
  
  T_THREAD_ARGS * threadArgs = (T_THREAD_ARGS *)arg;
  int currLine = 0;
  int inc = threadArgs->incremento;
  int inicio = threadArgs->inicio;
  char * arquivo = threadArgs->arquivo;
  char * linha = NULL;
  size_t len = 0;
  ssize_t read;
  char delimiter[] = ";";
  T_NODO * head = NULL;
  
  FILE * fp = fopen(arquivo, "r");
  
  while (currLine != inicio) {
    getline(&linha, &len, fp);
    ++currLine;
  }
  
  while (true) {
    
    if (getline(&linha, &len, fp) == EOF) {
      break;
    }
    
    if (linha == NULL || strlen(linha) < 10) {
      break;
    }
    
    char * nomeOrganismo = NULL;
    char * nomeGene = NULL;
    T_NODO * novo = NULL;
    int i;
    
    novo = (T_NODO *) malloc(sizeof(T_NODO));
    novo->proximo = NULL;
    
    novo->linha = (char *) malloc((strlen(linha) + 1) * sizeof(char));
    strcpy(novo->linha, linha);
    
    nomeOrganismo = strtok(linha, delimiter); 
    nomeGene = strtok(NULL, delimiter); 
    
    for (i = 0; nomeGene[i]; i++){
      nomeGene[i] = tolower(nomeGene[i]);
    }
    
    for (i = 0; nomeOrganismo[i]; i++){
      nomeOrganismo[i] = tolower(nomeOrganismo[i]);
    }
    
    novo->nomeGene = (char *) malloc((strlen(nomeGene) + 1) * sizeof(char));
    strcpy(novo->nomeGene, nomeGene);
    
    novo->nomeOrganismo = (char *) malloc((strlen(nomeOrganismo) + 1) * sizeof(char));
    strcpy(novo->nomeOrganismo, nomeOrganismo);
    
    sortedInsert(&(head), novo);
    
    ++currLine;
    for (i = 0; i < inc; i++) {
      getline(&linha, &len, fp);
      ++currLine;
    }
    
  }
  
  threadArgs->head_lista->head = head;
  
  fclose(fp);
  
}

void criaArquivoLista(T_NODO * head, char * nomeArquivo) {
  
  FILE * out = fopen(nomeArquivo, "w+");
  
  T_NODO * aux = head;
  
  while (aux != NULL) {
    fprintf(out, "%s", aux->linha);
    aux = aux->proximo;
  }
  
  fclose(out);
  
}

void main(int argc, char *argv[]) {
  
  if (argc != 3) {
    printf("%s <arquivo> <num_threads>\n", argv[0]);
    return;
  }
  
  char * arq = argv[1];
  int numThreads = atoi(argv[2]);
  int i;
  
  pthread_t * threads = malloc(sizeof(pthread_t) * numThreads);
  T_THREAD_ARGS * argmentos = malloc(sizeof(T_THREAD_ARGS) * numThreads);
  
  for (i = 0; i < numThreads; i++) {
    argmentos[i].incremento = numThreads - 1;
    argmentos[i].inicio = i;
    argmentos[i].arquivo = arq;
    argmentos[i].head_lista = (T_HEAD_LISTA *) malloc(sizeof(T_HEAD_LISTA));
    argmentos[i].head_lista->head = NULL;
    pthread_create(&(threads[i]), NULL, processaArquivo, &(argmentos[i]));
  }
  
  for (i = 0; i < numThreads; i++) {
    pthread_join(threads[i], NULL);
  }
  
  T_NODO * listaFinal = mergeLinkedLists(argmentos, numThreads);
  
  criaArquivoLista(listaFinal, "ordenado.csv");
  
  
}
