import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler, ActorContext}
import scala.concurrent.duration._

object CapteurVoie {

  sealed trait Command
  case object ArriveeVehicule extends Command
  case object FeuPasseAuVert extends Command
  case object TraiterProchainVehicule extends Command
  private case object GenererFlux extends Command

  private def genererTrajet(zoneInitiale: String): List[String] = {
    val probaContinuer = scala.util.Random.nextInt(100)
    if (probaContinuer < 60) { 
      val suivante = zoneInitiale match {
        case "2" => "1"
        case "1" => "3"
        case "3" => "4"
        case "4" => "2"
        case _   => zoneInitiale
      }
      List(zoneInitiale, suivante)
    } else {
      List(zoneInitiale)
    }
  }

  def apply(voieId: Int, zoneCible: String, hub: ActorRef[HubCentral.HubCommand]): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val fileInitiale = List.fill(scala.util.Random.nextInt(3))(genererTrajet(zoneCible))
        
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (5 + scala.util.Random.nextInt(7)).seconds)

        if (fileInitiale.nonEmpty) {
          hub ! HubCentral.DemandeTrajet(voieId, fileInitiale.head, fileInitiale.size, context.self)
        }

        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context) 
      }
    }
  }

  private def gestionFile(voieId: Int, zoneCible: String, file: List[List[String]], hub: ActorRef[HubCentral.HubCommand], timers: TimerScheduler[Command], context: ActorContext[Command]): Behavior[Command] = {
    Behaviors.receiveMessage {
      
      case ArriveeVehicule =>
        val nouveauTrajet = genererTrajet(zoneCible)
        val nouvelleFile = file :+ nouveauTrajet
        if (nouvelleFile.size == 1) {
          hub ! HubCentral.DemandeTrajet(voieId, nouveauTrajet, nouvelleFile.size, context.self)
        } else {
          // On informe quand même le hub pour l'affichage des ">"
          hub ! HubCentral.DemandeTrajet(voieId, file.head, nouvelleFile.size, context.self)
        }
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          timers.startSingleTimer(TraiterProchainVehicule, 1500.millis)
        }
        Behaviors.same

      case TraiterProchainVehicule =>
        if (file.nonEmpty) {
          val trajetActuel = file.head
          val zoneFinie = trajetActuel.head
          val resteDuTrajet = trajetActuel.tail

          if (resteDuTrajet.nonEmpty) {
            hub ! HubCentral.AvancerSequence(voieId, zoneFinie, resteDuTrajet.head, context.self)
            val fileMaj = resteDuTrajet :: file.tail
            gestionFile(voieId, zoneCible, fileMaj, hub, timers, context)
          } else {
            val fileApresSortie = file.tail
            hub ! HubCentral.FinPassageTotal(voieId, zoneFinie, fileApresSortie.size)
            if (fileApresSortie.nonEmpty) {
              hub ! HubCentral.DemandeTrajet(voieId, fileApresSortie.head, fileApresSortie.size, context.self)
            }
            gestionFile(voieId, zoneCible, fileApresSortie, hub, timers, context)
          }
        } else Behaviors.same

      case GenererFlux =>
        context.self ! ArriveeVehicule
        Behaviors.same
    }
  }
}