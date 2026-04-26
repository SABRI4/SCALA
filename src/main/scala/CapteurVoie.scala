import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler, ActorContext}
import scala.concurrent.duration._

object CapteurVoie {

  sealed trait Command
  case object ArriveeVehicule extends Command
  case object FeuPasseAuVert extends Command
  case object FeuPasseAuRouge extends Command
  case object TraiterProchainVehicule extends Command
  private case object GenererFlux extends Command

  def apply(voieId: Int, zoneCible: String, hub: ActorRef[HubCentral.HubCommand]): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val fileInitiale = List.fill(scala.util.Random.nextInt(4))("Auto")
        
        // Génération naturelle de trafic
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (4 + scala.util.Random.nextInt(6)).seconds)

        hub ! HubCentral.DemandeAcces(voieId, zoneCible, fileInitiale.size, context.self)
        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context) 
      }
    }
  }

  private def gestionFile(voieId: Int, zoneCible: String, file: List[String], hub: ActorRef[HubCentral.HubCommand], timers: TimerScheduler[Command], context: ActorContext[Command]): Behavior[Command] = {
    Behaviors.receiveMessage {
      
      case ArriveeVehicule =>
        val nouvelleFile = file :+ "Auto"
        hub ! HubCentral.DemandeAcces(voieId, zoneCible, nouvelleFile.size, context.self)
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          timers.startSingleTimer(TraiterProchainVehicule, 1200.millis)
          Behaviors.same
        } else {
          hub ! HubCentral.FinPassage(voieId, zoneCible, 0)
          Behaviors.same
        }

      case GenererFlux =>
        context.self ! ArriveeVehicule
        Behaviors.same

      case TraiterProchainVehicule =>
        if (file.nonEmpty) {
          val reste = file.tail
          
          // 1. Libérer la zone actuelle
          hub ! HubCentral.FinPassage(voieId, zoneCible, reste.size)

          // 2. Calcul de la zone suivante (Logique de quadrants)
          val zoneSuivante = zoneCible match {
            case "NE" => "NO"
            case "NO" => "SO"
            case "SO" => "SE"
            case "SE" => "NE"
            case _    => zoneCible
          }
          
          // On envoie le transfert au Hub
          hub ! HubCentral.TransfertVehicule(zoneSuivante)

          if (reste.nonEmpty) {
            hub ! HubCentral.DemandeAcces(voieId, zoneCible, reste.size, context.self)
            gestionFile(voieId, zoneCible, reste, hub, timers, context)
          } else {
            gestionFile(voieId, zoneCible, Nil, hub, timers, context)
          }
        } else Behaviors.same

      case FeuPasseAuRouge => Behaviors.same
    }
  }
}