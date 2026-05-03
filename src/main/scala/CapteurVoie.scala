import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler, ActorContext}
import scala.concurrent.duration._

object CapteurVoie {

  sealed trait Command
  case object ArriveeVehicule extends Command
  case object FeuPasseAuVert extends Command
  case object TraiterProchainVehicule extends Command
  case object FinChronoVert extends Command 
  case class PingPanne(replyTo: ActorRef[HubCentral.HubCommand]) extends Command
  private case object GenererFlux extends Command
  case class PingSante(replyTo: ActorRef[HubCentral.HubCommand]) extends Command

  private def genererTrajetFixe(voieId: Int, zoneCible: String): List[String] = {
    val direction = voieId % 3 
    direction match {
      case 1 => List(zoneCible) // Droite
      case _ => // Tout Droit ou Gauche
        val suivante = zoneCible match {
          case "2" => "1"; case "1" => "3"; case "3" => "4"; case "4" => "2"; case _ => zoneCible
        }
        List(zoneCible, suivante)
    }
  }

  def apply(voieId: Int, zoneCible: String, hub: ActorRef[HubCentral.HubCommand]): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val fileInitiale = List.fill(scala.util.Random.nextInt(1))(genererTrajetFixe(voieId, zoneCible))
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (7 + scala.util.Random.nextInt(7)).seconds)

        if (fileInitiale.nonEmpty) {
          hub ! HubCentral.DemandeTrajet(voieId, fileInitiale.head, fileInitiale.size, context.self)
        }
        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context, 0L) 
      }
    }
  }

  private def gestionFile(
    voieId: Int, zoneCible: String, file: List[List[String]], hub: ActorRef[HubCentral.HubCommand],
    timers: TimerScheduler[Command], context: ActorContext[Command], debutVert: Long
  ): Behavior[Command] = {

    Behaviors.receiveMessage {
      case PingPanne(replyTo) =>
        
        replyTo ! HubCentral.PongPanne(voieId)
        Behaviors.same

      case PingSante(replyTo) =>
        replyTo ! HubCentral.PongSante(voieId)
        Behaviors.same

      case ArriveeVehicule =>
        val nouvelleFile = file :+ genererTrajetFixe(voieId, zoneCible)
        hub ! HubCentral.DemandeTrajet(voieId, (if (file.isEmpty) nouvelleFile.head else file.head), nouvelleFile.size, context.self)
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context, debutVert)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          val nouveauDebut = if (debutVert == 0L) {
            timers.startSingleTimer(FinChronoVert, 10.seconds)
            System.currentTimeMillis()
          } else debutVert
          timers.startSingleTimer(TraiterProchainVehicule, 100.millis)
          gestionFile(voieId, zoneCible, file, hub, timers, context, nouveauDebut)
        } else {
          hub ! HubCentral.FinPassageTotal(voieId, zoneCible, 0)
          gestionFile(voieId, zoneCible, Nil, hub, timers, context, 0L)
        }

    case TraiterProchainVehicule =>
      if (file.nonEmpty) {
        val trajet = file.head
        val zoneFinie = trajet.head
        val reste = trajet.tail

        if (reste.nonEmpty) {
          // La voiture avance dans sa séquence (elle ne quitte pas le carrefour pour le moment)
          hub ! HubCentral.AvancerSequence(voieId, zoneFinie, reste.head, context.self)
          gestionFile(voieId, zoneCible, reste :: file.tail, hub, timers, context, debutVert)
        } 
        else {
          // La voiture a finit son trajet complet
          val fileApres = file.tail
          val temps = System.currentTimeMillis() - debutVert

          //Libération de la zone si on n'en a plus besoin pour notre trajet
          hub ! HubCentral.FinPassageTotal(voieId, zoneFinie, fileApres.size)

          if (fileApres.nonEmpty && temps < 10000) {
            // On demande pour la suivante, mais la zone précédente est déjà libre !
            hub ! HubCentral.DemandeTrajet(voieId, fileApres.head, fileApres.size, context.self)
            gestionFile(voieId, zoneCible, fileApres, hub, timers, context, debutVert)
          } else {
            // On a fini notre temps de parole ou plus de voitures
            timers.cancel(FinChronoVert)
            gestionFile(voieId, zoneCible, fileApres, hub, timers, context, 0L)
          }
        }
      } else Behaviors.same

      // On remet le compteur a 0 soit feu rouge meme si il y a encore des véhicules sur la voie associé au capteur
      case FinChronoVert => gestionFile(voieId, zoneCible, file, hub, timers, context, 0L)
      case GenererFlux => context.self ! ArriveeVehicule; Behaviors.same
    }
  }
}