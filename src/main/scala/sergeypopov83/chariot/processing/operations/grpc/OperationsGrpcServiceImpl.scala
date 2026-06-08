package sergeypopov83.chariot.processing.operations.grpc

import io.grpc.{Status, StatusException}
import sergeypopov83.chariot.processing.operations.grpc.OperationsService.*
import sergeypopov83.chariot.processing.operations.repository.mongodb.OperationsRepository
import sergeypopov83.chariot.processing.operations.service.grpc.{SponsorbankService, toGrpc}
import zio.{IO, ZIO}

class OperationsGrpcServiceImpl(operRepo: OperationsRepository.Service) extends ZioOperationsService.OperationsGrpcService {

  override def operationsPeriodStatisticsByLegalEntity(request: OperationsPeriodStatisticsByLegalEntityRequest):
  IO[StatusException, OperationsPeriodStatisticsByLegalEntityResponse] = (
    for {
      statOpt <- operRepo.operationsPeriodStatisticsByLegalEntity(request.legalEntity, 
        request.periodStart.map(_.asJavaInstant), request.periodEnd.map(_.asJavaInstant))
      stat = statOpt.getOrElse(throw RuntimeException("Not found"))
    } yield {
      OperationsPeriodStatisticsByLegalEntityResponse.of (
            legalEntity = request.legalEntity, periodStart = Option(stat.periodStart.toGrpc), 
        periodEnd = Option(stat.periodEnd.toGrpc), count = stat.count ,
        operationsAmount = Option(stat.totalAmount.toGrpc)
      )
    }).catchAllCause { th =>
    ZIO.logError(s"Method failed with $th").flatMap(_ =>
      ZIO.fail(Status.FAILED_PRECONDITION.withDescription(th.prettyPrint).withCause(th.squash).asException())
    )
  }

  override def topNOperationsByAmount(request: TopNOperationsByAmountRequest):
  IO[StatusException, TopNOperationsByAmountResponse] = ???


  override def lastNOperations(request: LastNOperationsRequest):
  IO[StatusException, LastNOperationsResponse] = ???

}
