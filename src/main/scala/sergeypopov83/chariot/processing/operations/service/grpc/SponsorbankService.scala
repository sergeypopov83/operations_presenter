package sergeypopov83.chariot.processing.operations.service.grpc

import sergeypopov83.chariot.processing.sponsorbank.grpc.TransfersService.{GetTransferIntoByTransferIdRequest, GetTransferIntoByTransferIdResponse}
import sergeypopov83.chariot.processing.sponsorbank.grpc.TransfersService.ZioTransfersService.{TransfersServiceClient, ZTransfersService}
import zio.Task

import java.util.UUID

class SponsorbankService(client: TransfersServiceClient) {

 def findTransferData(tId: UUID): Task[GetTransferIntoByTransferIdResponse] =
  client.getTransferIntoByTransferId(GetTransferIntoByTransferIdRequest.of(tId.toString)).mapError(se =>
   new RuntimeException(se.getMessage, se.getCause)
  )

}
