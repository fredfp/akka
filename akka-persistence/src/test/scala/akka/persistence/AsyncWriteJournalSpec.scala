/*
 * Copyright (C) 2009-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor._
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit.ImplicitSender

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try


object AsyncWriteJournalSpec {
  import PersistentActorSpec.{Cmd, Evt, ExamplePersistentActor}

  class FailingInmemJournal extends InmemJournal {

    override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
      println(messages)
//      Thread.sleep(10)
      messages.head.persistenceId // will throw on empty messages
      println("boom")
      Future { Thread.sleep(1); Nil}(scala.concurrent.ExecutionContext.global)
//      Future.successful(Nil)
    }
  }

  class DeferringPersistentActor(name: String) extends ExamplePersistentActor(name) {
    override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      cause.printStackTrace()
      context.stop(self)
    }

    val receiveCommand: Receive = commonBehavior.orElse {
      case Cmd("get") =>
        deferAsync(()) { _ =>
          sender() ! events.reverse
        }
      case Cmd("defer") =>
        persistAllAsync(Nil) { _ =>}
        deferAsync(()) { _ =>
        Thread.sleep(1)
      }
      case Cmd(txt) =>
        persistAllAsync(List(Evt(txt)))(updateState)
        deferAsync(()) { _ =>
          Thread.sleep(1)
        }
    }
  }
}

class AsyncWriteJournalSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "AsyncWriteMessagesSpec",
        extraConfig = Some(
          """
  akka.persistence.journal-plugin-fallback.circuit-breaker.max-failures=1
  akka.persistence.journal.inmem.class = "akka.persistence.AsyncWriteJournalSpec$FailingInmemJournal"
  """)))
    with ImplicitSender {

  import AsyncWriteJournalSpec._
  import PersistentActorSpec._

  "AsyncWriteJournal" must {
    "not pass empty messages to the journal plugin" in {
      val persistentActor = namedPersistentActor[DeferringPersistentActor]
      watch(persistentActor)
      persistentActor ! Cmd("test")
      for { _ <- 0 to 1000 } {
      persistentActor ! Cmd("defer")
        Thread.sleep(1)
      }
      persistentActor ! GetState
      expectMsg(List("test"))
      persistentActor ! Cmd("test2")
      persistentActor ! Cmd("get")
      expectMsg(List("test", "test2"))
      expectTerminated(persistentActor)
    }
  }
}
