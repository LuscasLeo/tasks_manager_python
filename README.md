# Python Tasks Manager Service

## Propósito

Aplicação para gerenciar e monitorar tarefas que são executadas de forma assíncrona.

A Aplicação deve gerenciar **Tarefas** e **Execuções** a para fins de segregação e organização de execuções de uma ação ou um mesmo tipo de ação.

## Necessidades
- A Aplicação deve ser flexível a receber mútiplos tipos de tarefas e inputs e possibilitar integração de mútiplos **módulos**
- A Aplicação deve possibilitar a observação de suas tarefas em execução através de *Logs* enviados de seus módulos

## Entidades

- ### Módulo
   Objeto que guarda a regra de negócio da ação a ser executada.
   Ex:
   #### SAY_HELLO
   ```python

    class SayHelloPayload:
        name: str

    def say_hello(payload: SayHelloPayload):
        print(f'Hello, {name}!')
   ```

- ### Tarefa
    Objeto que descreve a ação a ser executada, através de parâmetros
    Ex:
    ```json
    {
        "type": "SAY_HELLO",
        "payload": {
            "name": "Lucas"
        }
    }
    
    ```


- ### Execução
  Objeto que agrega uma **Tarefa** e é utilizado por um **Módulo**.
  É utilizado como um registro de uma execução de tarefa.

  Ex:
  ```json
  {
      "id": "0196d2dc-0dac-474a-a04b-b4d7d924e25a",
      "task": {
          "type": "SAY_HELLO",
          "payload": {
              "name": "Lucas"
          }
      },
      "logs": [
          {
              "timestamp": "1647629469423",
              "level": "INFO",
              "message": "Said Hello, Lucas!",
              "loggerName": "Module"
          },
          {
              "timestamp": "1647629601735",
              "level": "ERROR",
              "message": "Couldn't say Hello, Lucas!",
              "loggerName": "Module",
              "error": "CantSayHelloException",
              "stackTrace": ""
          }
      ]
  }
  ```
