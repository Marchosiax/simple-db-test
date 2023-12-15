#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    char *buffer;
    size_t bufferLength;
    ssize_t inputLength;
} InputBuffer;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_SELECT,
    STATEMENT_INSERT,
    STATEMENT_UPDATE,
    STATEMENT_DELETE
} StatementType;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

InputBuffer *newInputBuffer() {
    InputBuffer *input = (InputBuffer *) malloc(sizeof(InputBuffer));
    input->buffer = NULL;
    input->bufferLength = 0;
    input->inputLength = 0;
    return input;
}

void closeInputBuffer(InputBuffer *inputBuffer) {
    free(inputBuffer->buffer);
    free(inputBuffer);
}

void printPrompt() {
    printf("db > ");
}

void readInput(InputBuffer *inputBuffer) {
    ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->bufferLength), stdin);
    if (bytesRead <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    inputBuffer->inputLength = bytesRead - 1;
    inputBuffer->buffer[bytesRead - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer *input) {
    if (strcmp(input->buffer, ".exit") == 0) {
        // Could return META_COMMAND_EXIT
        closeInputBuffer(input);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
}

PrepareResult prepare_statement(InputBuffer *input, Statement *statement) {
    if (strcmp(input->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    if (strcmp(input->buffer, "insert") == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
                                   statement->row_to_insert.username, statement->row_to_insert.email);
        if(args_assigned<0){
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strcmp(input->buffer, "update") == 0) {
        statement->type = STATEMENT_UPDATE;
        return PREPARE_SUCCESS;
    }
    if (strcmp(input->buffer, "delete") == 0) {
        statement->type = STATEMENT_DELETE;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
    switch (statement->type) {
        case STATEMENT_SELECT:
            printf("Select done\n");
            break;
        case STATEMENT_INSERT:
            printf("Insert done\n");
            break;
        case STATEMENT_UPDATE:
            printf("Update done\n");
            break;
        case STATEMENT_DELETE:
            printf("Delete done\n");
            break;
    }
}

int main(void) {
    InputBuffer *input = newInputBuffer();
    while (true) {
        printPrompt();
        readInput(input);

        // Check for meta commands
        if (input->buffer[0] == '.') {
            switch (do_meta_command(input)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED:
                    printf("Unrecognized command '%s'\n", input->buffer);
                    continue;
            }
        }

        Statement statement;
        PrepareResult prepare_result = prepare_statement(input, &statement);
        switch (prepare_result) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at '%s'\n", input->buffer);
                break;
        }
        execute_statement(&statement);
    }
}
