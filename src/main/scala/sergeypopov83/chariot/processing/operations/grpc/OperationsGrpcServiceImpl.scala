package sergeypopov83.chariot.processing.operations.grpc

import io.grpc.StatusException
import sergeypopov83.chariot.processing.operations.grpc.operations.*
import zio.IO

class OperationsGrpcServiceImpl extends ZioOperations.OperationsGrpcService {

  override def operationsPeriodStatisticsByLegalEntity(request: OperationsPeriodStatisticsByLegalEntityRequest):
  IO[StatusException, OperationsPeriodStatisticsByLegalEntityResponse] = ???

  override def topNOperationsByAmount(request: TopNOperationsByAmountRequest):
  IO[StatusException, TopNOperationsByAmountResponse] = ???


  override def lastNOperations(request: LastNOperationsRequest):
  IO[StatusException, LastNOperationsResponse] = ???

}
