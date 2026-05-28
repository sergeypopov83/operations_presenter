package sergeypopov83.chariot.processing.operations.service.scheduler

import zio.{Schedule, ZIO, given}

object ReaderScheduler {

  private val policy = Schedule.spaced(50.milliseconds)

  def scheduleWithAction[A, B, C](t: ZIO[A, B, C]): ZIO[A, B, Long] =
    t.catchSomeCause {
      case cause => ZIO.logWarning(s"Reschedule failed with $cause")
    }.repeat(policy)

}
