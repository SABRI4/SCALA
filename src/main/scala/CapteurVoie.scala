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


  private def genererTrajetFixe(voieId: Int, zoneCible: String): List[String] = {
    val direction = voieId % 3 

    direction match {
      case 1 => 
        // VIRAGE À DROITE : 1 seule zone (la zone d'entrée)
        List(zoneCible)

      case _ => 
        // TOUT DROIT ou GAUCHE : 2 zones
        val suivante = zoneCible match {
          case "2" => "1" // Nord-Est vers Nord-Ouest
          case "1" => "3" // Nord-Ouest vers Sud-Ouest
          case "3" => "4" // Sud-Ouest vers Sud-Est
          case "4" => "2" // Sud-Est vers Nord-Est
          case _   => zoneCible
        }
        List(zoneCible, suivante)
    }
  }

  //Le "constructeur" de notre acteur
  def apply(voieId: Int, zoneCible: String, hub: ActorRef[HubCentral.HubCommand]): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>

        val fileInitiale = List.fill(scala.util.Random.nextInt(3))(genererTrajetFixe(voieId, zoneCible))
        
        // On fixe le fait qu'une voiture arrive toutes les 5 à 12 secondes
        timers.startTimerWithFixedDelay(GenererFlux, ArriveeVehicule, (5 + scala.util.Random.nextInt(7)).seconds)

        // Si la file n'est pas vide, on demande immédiatement le passage au Hub
        if (fileInitiale.nonEmpty) {
          hub ! HubCentral.DemandeTrajet(voieId, fileInitiale.head, fileInitiale.size, context.self)
        }
        
        //On recevra notre propre commande ici afin de gérer les voitures lors de l'instanciation de notre acteur
        gestionFile(voieId, zoneCible, fileInitiale, hub, timers, context, 0L) 
      }
    }
  }

  // Gestion de l'arrivé des voitures (demandes d'accès etc...)
  private def gestionFile(
    voieId: Int, 
    zoneCible: String, 
    file: List[List[String]], 
    hub: ActorRef[HubCentral.HubCommand],
    timers: TimerScheduler[Command], 
    context: ActorContext[Command],
    debutVert: Long // Timestamp pour gérer le cycle des 10 secondes
  ): Behavior[Command] = {

    Behaviors.receiveMessage {
      case ArriveeVehicule =>
        //création d'une nouvelle file avec la nouvelle voiture
        val nouvelleFile = file :+ genererTrajetFixe(voieId, zoneCible)
        // On informe le Hub pour mettre à jour l'affichage des ">"
        if (nouvelleFile.size == 1) {
          hub ! HubCentral.DemandeTrajet(voieId, nouvelleFile.head, nouvelleFile.size, context.self)
        } 
        else {
          hub ! HubCentral.DemandeTrajet(voieId, file.head, nouvelleFile.size, context.self)
        }
        gestionFile(voieId, zoneCible, nouvelleFile, hub, timers, context, debutVert)

      case FeuPasseAuVert =>
        if (file.nonEmpty) {
          // Si c'est le début d'un nouveau cycle de feu vert, on lance le chrono
          val nouveauDebut = if (debutVert == 0L) {
            val maintenant = System.currentTimeMillis()
            timers.startSingleTimer(FinChronoVert, 10.seconds)
            maintenant
          } else debutVert

          // 1s pour traversé une zone
          timers.startSingleTimer(TraiterProchainVehicule, 1000.millis)
          gestionFile(voieId, zoneCible, file, hub, timers, context, nouveauDebut)
        } 
        
        else {
          // Plus personne en file, on rend la main au Hub ducoup
          hub ! HubCentral.FinPassageTotal(voieId, zoneCible, 0)
          gestionFile(voieId, zoneCible, Nil, hub, timers, context, 0L)
        }

      case TraiterProchainVehicule =>
        if (file.nonEmpty) {
          val trajetActuel = file.head
          val zoneFinie = trajetActuel.head
          val resteDuTrajet = trajetActuel.tail

          if (resteDuTrajet.nonEmpty) {
            // Le véhicule avance dans la zone suivante du carrefour
            hub ! HubCentral.AvancerSequence(voieId, zoneFinie, resteDuTrajet.head, context.self)
            gestionFile(voieId, zoneCible, resteDuTrajet :: file.tail, hub, timers, context, debutVert)
          } 
          
          else {
            // Le véhicule vient de sortir du carrefour
            val fileApresSortie = file.tail
            val tempsEcoule = System.currentTimeMillis() - debutVert
            
            // On continue si la file n'est pas vide ET qu'il reste du temps sur les 10s
            if (fileApresSortie.nonEmpty && tempsEcoule < 10000) {
              timers.startSingleTimer(FeuPasseAuVert, 400.millis) // Petit délai entre deux voitures afin de simuler
              gestionFile(voieId, zoneCible, fileApresSortie, hub, timers, context, debutVert)
            } else {
              // Fin du temps imparti ou file vide : passage au rouge
              timers.cancel(FinChronoVert)
              hub ! HubCentral.FinPassageTotal(voieId, zoneFinie, fileApresSortie.size)
              
              // Si la file n'est pas vide, on se remet en attente de réservation
              if (fileApresSortie.nonEmpty) {
                hub ! HubCentral.DemandeTrajet(voieId, fileApresSortie.head, fileApresSortie.size, context.self)
              }
              gestionFile(voieId, zoneCible, fileApresSortie, hub, timers, context, 0L)
            }
          }
        } else Behaviors.same

      case FinChronoVert =>
        // Le temps est écoulé, le prochain véhicule passera par la branche else de TraiterProchainVehicule
        Behaviors.same

      case GenererFlux =>
        context.self ! ArriveeVehicule
        Behaviors.same
    }
  }
}