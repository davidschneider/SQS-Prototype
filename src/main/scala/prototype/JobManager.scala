package prototype

import akka.actor.{Props, ActorSystem, Actor}

import scala.collection.JavaConversions._
import scala.concurrent.Future

import scala.concurrent.duration._

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model._

import scala.util.Random

case class NewJob(m: Message)

case class NewStatus(id: String, status: String)

case class PollNewJob(q: String)

case class PollNewStatus(q: String)

case object Stop

object JobManager {

  val credentials = new ProfileCredentialsProvider().getCredentials

  val sqs = new AmazonSQSClient(credentials)
  val usEast1 = Region.getRegion(Regions.US_EAST_1)
  sqs.setRegion(usEast1)

  val system = ActorSystem("JobManager")
  val jm = system.actorOf(Props[JobManagerActor])
  val jp = system.actorOf(Props[JobPollingActor])
  val sp = system.actorOf(Props[StatusPollingActor])

  implicit val executor = system.dispatcher

  class JobPollingActor extends Actor {
    def receive = {
      case PollNewJob(q) =>
        val rmr = new ReceiveMessageRequest().withQueueUrl(q).withWaitTimeSeconds(20)
        val rm = sqs.receiveMessage(rmr)
        for (m <- rm.getMessages) {
          jm ! NewJob(m)
          sqs.deleteMessage(new DeleteMessageRequest(q, m.getReceiptHandle))
        }
        self ! PollNewJob(q)
    }
  }

  class StatusPollingActor extends Actor {
    def receive = {
      case PollNewStatus(q) =>
        val rmr = new ReceiveMessageRequest().withQueueUrl(q).withWaitTimeSeconds(20)
        val rm = sqs.receiveMessage(rmr)
        for (m <- rm.getMessages) {
          jm ! NewStatus(m.getMessageId, m.getBody)
          sqs.deleteMessage(new DeleteMessageRequest(q, m.getReceiptHandle))
        }
        self ! PollNewStatus(q)
    }
  }

  class JobManagerActor extends Actor {

    def receive = {
      case NewJob(m) =>
        System.out.println("received new job: " + m.getBody)
        sqs.sendMessage("w_newjob", m.getBody)
        if (m.getBody.equalsIgnoreCase("stop")) self ! Stop
      case NewStatus(m, s) => System.out.println("received new status: " + s)
      case Stop =>
        system.shutdown()
        System.exit(1)
    }

  }

  def main(args: Array[String]): Unit = {

    system.scheduler.scheduleOnce(0 seconds) {
      jp ! PollNewJob("jm_newjob")
      sp ! PollNewStatus("jm_status")
    }

  }

}

object Worker {

  val random = new Random()

  val credentials = new ProfileCredentialsProvider().getCredentials

  val sqs = new AmazonSQSClient(credentials)
  val usEast1 = Region.getRegion(Regions.US_EAST_1)

  val system = ActorSystem("Worker")
  val w = system.actorOf(Props[WorkerActor])
  val p = system.actorOf(Props[PollingActor])

  implicit val executor = system.dispatcher

  class PollingActor extends Actor {
    def receive = {
      case PollNewJob(q) =>
        val rmr = new ReceiveMessageRequest().withQueueUrl(q).withWaitTimeSeconds(20)
        val wm = sqs.receiveMessage(rmr)
        System.out.println(s"received ${wm.getMessages.size} messages")
        for (m <- wm.getMessages) {
          w ! NewJob(m)
          sqs.deleteMessage(new DeleteMessageRequest(q, m.getReceiptHandle))
        }
        self ! PollNewJob(q)
    }
  }

  class WorkerActor extends Actor {

    def receive = {
      case NewJob(m) =>
        Future {
          System.out.println("received new job: " + m.getBody)
          sqs.sendMessage("jm_status", "running")
          Thread.sleep((random.nextInt(9) + 1) * 1000)
          sqs.sendMessage("jm_status", "finished")
          if (m.getBody.equalsIgnoreCase("stop")) self ! Stop
        }
      case Stop =>
        system.shutdown()
        System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    system.scheduler.scheduleOnce(0 seconds) {
      p ! PollNewJob("w_newjob")
    }

  }

}

object Test {

  val credentials = new ProfileCredentialsProvider().getCredentials

  val sqs = new AmazonSQSClient(credentials)
  val usEast1 = Region.getRegion(Regions.US_EAST_1)

  def main(args: Array[String]): Unit = {

    for (i <- 1 until 1000) {

      sqs.sendMessage("jm_newjob", i.toString)

    }

    //    sqs.sendMessage("jm_newjob", "stop")

  }

}