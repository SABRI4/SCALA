import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object HubCentral {

  sealed trait HubCommand
  case class DemandeAcces(voieId: Int, zoneCible: String, nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassage(voieId: Int, zoneCible: String, nbVehicules: Int) extends HubCommand
  case class TransfertVehicule(zoneSuivante: String) extends HubCommand

  case class EtatCarrefour(
    occupations: Map[String, Boolean],
    filesAttente: Map[Int, Int],
    registreCapteurs: Map[String, ActorRef[CapteurVoie.Command]] // Pour savoir où envoyer les transferts
  )

  def apply(): Behavior[HubCommand] = {
    val etatInitial = EtatCarrefour(
      Map("NE" -> false, "NO" -> false, "SE" -> false, "SO" -> false),
      (1 to 16).map(_ -> 0).toMap,
      Map.empty
    )
    gestionnaire(etatInitial)
  }

  private def gestionnaire(etat: EtatCarrefour): Behavior[HubCommand] = Behaviors.receive { (context, message) =>
    val nouvelEtat = message match {
      case DemandeAcces(id, zone, nb, replyTo) =>
        val estOccupee = etat.occupations.getOrElse(zone, true)
        val filesMaj = etat.filesAttente + (id -> nb)
        // On enregistre quel capteur gère quelle zone (le dernier demandeur gagne)
        val registreMaj = etat.registreCapteurs + (zone -> replyTo)

        if (!estOccupee) {
          replyTo ! CapteurVoie.FeuPasseAuVert
          etat.copy(occupations = etat.occupations + (zone -> true), filesAttente = filesMaj, registreCapteurs = registreMaj)
        } else {
          etat.copy(filesAttente = filesMaj, registreCapteurs = registreMaj)
        }

      case FinPassage(id, zone, nb) =>
        etat.copy(
          occupations = etat.occupations + (zone -> false),
          filesAttente = etat.filesAttente + (id -> nb)
        )

      case TransfertVehicule(zoneSuivante) =>
        // On trouve le capteur qui gère la zone suivante et on lui envoie une voiture
        etat.registreCapteurs.get(zoneSuivante).foreach { capteurRef =>
          capteurRef ! CapteurVoie.ArriveeVehicule
        }
        etat
    }

    afficherCarrefour(nouvelEtat)
    gestionnaire(nouvelEtat)
  }

  private def afficherCarrefour(etat: EtatCarrefour): Unit = {
    print("\u001b[2J\u001b[H")
    println("=====================================================")
    println("       MONITORING CARREFOUR CRITIQUE 2026           ")
    println("=====================================================")
    
    print("ZONES : ")
    List("NE", "NO", "SE", "SO").foreach { z =>
      val status = if (etat.occupations.getOrElse(z, false)) "[OCCUPE]" else "[LIBRE ]"
      print(s"$z: $status   ")
    }
    println("\n" + "-" * 53)

    for (i <- 1 to 12) {
      val nb = etat.filesAttente.getOrElse(i, 0)
      val voitures = ">" * nb
      print(f"Voie $i%02d: $voitures%-10s | ")
      if (i % 4 == 0) println()
    }
    println("=====================================================")
    println(">> Appuyez sur Ctrl+C pour arrêter")
  }
}