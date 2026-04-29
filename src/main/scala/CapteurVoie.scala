import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler, ActorContext}
import scala.concurrent.duration._

object CapteurVoie {

  sealed trait Command
  case object ArriveeVehicule extends Command
  case object FeuPasseAuVert extends Command
  case object TraiterProchainVehicule extends Command
  case object FinChronoVert extends Command 
  private case object GenererFlux extends Command

  // Génération de trajet : soit traverse la zone, soit enchaîne sur la suivante
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
        // File de départ avec trajets aléatoires
        val fileInitiale = List.fill(scala.util.Random.nextInt(3))(genererTrajet(zoneCible))
        
        // Flux d'arrivée des voitures (toutes les 5 à 12 secondes)
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (5 + scala.util.Random.nextInt(7)).seconds)

        // Si on a déjà du monde, on demande le trajet au Hub
        if (fileInitiale.nonEmpty) {
          hub ! HubCentral.DemandeTrajet(voieId, fileInitiale.head, fileInitiale.size, context.self)
        }

        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context, 0L) 
      }
    }
  }

  private def gestionFile(
    voieId: Int, 
    zoneCible: String, 
    file: List[List[String]], 
    hub: ActorRef[HubCentral.HubCommand],
    timers: TimerScheduler[Command], 
    context: ActorContext[Command],
    debutVert: Long
  ): Behavior[Command] = {

    Behaviors.receiveMessage {
      case ArriveeVehicule =>
        val nouvelleFile = file :+ genererTrajet(zoneCible)
        // On informe le Hub de la nouvelle taille de file pour l'affichage
        if (nouvelleFile.size == 1) {
          hub ! HubCentral.DemandeTrajet(voieId, nouvelleFile.head, nouvelleFile.size, context.self)
        } else {
          // Mise à jour visuelle des ">"
          hub ! HubCentral.DemandeTrajet(voieId, file.head, nouvelleFile.size, context.self)
        }
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context, debutVert)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          // Démarrage du chrono de 10s si c'est le début du cycle
          val nouveauDebut = if (debutVert == 0L) {
            val maintenant = System.currentTimeMillis()
            timers.startSingleTimer(FinChronoVert, 10.seconds)
            maintenant
          } else debutVert

          timers.startSingleTimer(TraiterProchainVehicule, 1200.millis)
          gestionFile(voieId, zoneCible, file, hub, timers, context, nouveauDebut)
        } else {
          // Personne ? On rend le jeton au Hub immédiatement
          hub ! HubCentral.FinPassageTotal(voieId, zoneCible, 0)
          gestionFile(voieId, zoneCible, Nil, hub, timers, context, 0L)
        }

      case TraiterProchainVehicule =>
        if (file.nonEmpty) {
          val trajetActuel = file.head
          val zoneFinie = trajetActuel.head
          val resteDuTrajet = trajetActuel.tail

          if (resteDuTrajet.nonEmpty) {
            // Le véhicule avance d'une zone (transfert interne)
            hub ! HubCentral.AvancerSequence(voieId, zoneFinie, resteDuTrajet.head, context.self)
            gestionFile(voieId, zoneCible, resteDuTrajet :: file.tail, hub, timers, context, debutVert)
          } else {
            val fileApresSortie = file.tail
            val tempsEcoule = System.currentTimeMillis() - debutVert
            
            // On continue si : il y a des voitures ET on est dans les 10s
            if (fileApresSortie.nonEmpty && tempsEcoule < 10000) {
              timers.startSingleTimer(FeuPasseAuVert, 400.millis)
              gestionFile(voieId, zoneCible, fileApresSortie, hub, timers, context, debutVert)
            } else {
              // Fin de cycle : temps écoulé ou file vide
              timers.cancel(FinChronoVert)
              hub ! HubCentral.FinPassageTotal(voieId, zoneFinie, fileApresSortie.size)
              
              if (fileApresSortie.nonEmpty) {
                hub ! HubCentral.DemandeTrajet(voieId, fileApresSortie.head, fileApresSortie.size, context.self)
              }
              gestionFile(voieId, zoneCible, fileApresSortie, hub, timers, context, 0L)
            }
          }
        } else Behaviors.same

      case FinChronoVert =>
        // Le message est reçu mais la logique est gérée par la vérification de 'tempsEcoule'
        Behaviors.same

      case GenererFlux =>
        context.self ! ArriveeVehicule
        Behaviors.same
    }
  }
}