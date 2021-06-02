class IntegrationError(Exception):
    pass


class InvalidDataType(IntegrationError):
    pass


class TableNotFound(IntegrationError):
    pass


class ColumnNotFound(IntegrationError):
    pass


class InvalidInputData(IntegrationError):
    pass


class InvalidPartitionData(IntegrationError):
    pass


class WorkflowFunctionError(IntegrationError):
    pass
