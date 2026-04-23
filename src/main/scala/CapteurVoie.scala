import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler, ActorContext}
import scala.concurrent.duration._

object CapteurVoie {

  //Les différentes commandes que l'on peut envoyer au capteur de voie
  sealed trait Command
  case object ArriveeVehicule extends Command
  case object FeuPasseAuVert extends Command
  case object FeuPasseAuRouge extends Command
  case object TraiterProchainVehicule extends Command
  private case object GenererFlux extends Command

  def apply(voieId: Int, zoneCible: String, hub: ActorRef[HubCentral.HubCommand]): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        // Ici on initialise une file de départ aléatoire
        val fileInitiale = List.fill(scala.util.Random.nextInt(4))("Auto")
        
        // Démarrage de la génération de véhicules par notre même objet toutes les 4 à 10 secondes 
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (4 + scala.util.Random.nextInt(6)).seconds)

        // On informe l'acteur Hub de notre état initial pour l'affichage
        hub ! HubCentral.DemandeAcces(voieId, zoneCible, fileInitiale.size, context.self)

        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context) 
      }
    }
  }

  private def gestionFile(voieId: Int,zoneCible: String, file: List[String], hub: ActorRef[HubCentral.HubCommand],timers: TimerScheduler[Command], context: ActorContext[Command]): Behavior[Command] = {
    Behaviors.receiveMessage {
      
      case ArriveeVehicule =>
        val nouvelleFile = file :+ "Auto"
        // On prévient le Hub qu'un nouveau véhicule est arrivé (pour l'affichage et la demande)
        hub ! HubCentral.DemandeAcces(voieId, zoneCible, nouvelleFile.size, context.self)
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          // Simulation du temps de traversée
          timers.startSingleTimer(TraiterProchainVehicule, 1200.millis)
          Behaviors.same
        } else {
          hub ! HubCentral.FinPassage(voieId, zoneCible, 0)
          Behaviors.same
        }

      case GenererFlux =>
        // Génère un nouveau véhicule à intervalle régulier
        context.self ! ArriveeVehicule
        Behaviors.same

      case TraiterProchainVehicule =>
        if (file.nonEmpty) {
          val reste = file.tail
          if (reste.nonEmpty) {
            // Le véhicule est sorti, on met à jour le Hub et on continue si le feu est tjs vert
            hub ! HubCentral.DemandeAcces(voieId, zoneCible, reste.size, context.self)
            context.self ! FeuPasseAuVert
            gestionFile(voieId, zoneCible, reste, hub, timers, context)
          } else {
            // Plus personne, on rend le jeton de zone
            hub ! HubCentral.FinPassage(voieId, zoneCible, 0)
            gestionFile(voieId, zoneCible, Nil, hub, timers, context)
          }
        } else Behaviors.same

      case FeuPasseAuRouge =>
        Behaviors.same
    }
  }
}