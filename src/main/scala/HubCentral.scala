import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object HubCentral {

  sealed trait HubCommand
  case class DemandeTrajet(voieId: Int, trajet: List[String], nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class AvancerSequence(voieId: Int, zoneQuittee: String, zoneEntree: String, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassageTotal(voieId: Int, derniereZone: String, nbVehicules: Int) extends HubCommand

  case class EtatCarrefour(
    reservations: Map[String, Int], 
    filesAttente: Map[Int, Int]
  )

  def apply(): Behavior[HubCommand] = {
    val etatInitial = EtatCarrefour(
      reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0), 
      filesAttente = (1 to 12).map(_ -> 0).toMap
    )
    gestionnaire(etatInitial)
  }

  private def gestionnaire(etat: EtatCarrefour): Behavior[HubCommand] = Behaviors.receive { (context, message) =>
    val nouvelEtat = message match {
      case DemandeTrajet(id, trajet, nb, replyTo) =>
        val zonesLibres = trajet.forall(z => etat.reservations.getOrElse(z, 0) == 0)
        val filesMaj = etat.filesAttente + (id -> nb) // Mise à jour affichage

        if (zonesLibres) {
          val nouvellesRes = etat.reservations ++ trajet.map(_ -> id)
          replyTo ! CapteurVoie.FeuPasseAuVert
          etat.copy(reservations = nouvellesRes, filesAttente = filesMaj)
        } else {
          etat.copy(filesAttente = filesMaj)
        }

      case AvancerSequence(id, quittee, entree, replyTo) =>
        val nouvellesRes = etat.reservations + (quittee -> 0)
        replyTo ! CapteurVoie.FeuPasseAuVert
        etat.copy(reservations = nouvellesRes)

      case FinPassageTotal(id, derniere, nb) =>
        val nouvellesRes = etat.reservations + (derniere -> 0)
        val filesMaj = etat.filesAttente + (id -> nb) // Mise à jour affichage
        etat.copy(reservations = nouvellesRes, filesAttente = filesMaj)
    }

    afficherCarrefour(nouvelEtat)
    gestionnaire(nouvelEtat)
  }

  private def afficherCarrefour(etat: EtatCarrefour): Unit = {
    print("\u001b[2J\u001b[H")
    println("=====================================================")
    println("       MONITORING CARREFOUR CRITIQUE 2026           ")
    println("=====================================================")
    
    print("ZONES (Verrous) : ")
    List("1", "2", "3", "4").foreach { z =>
      val verrou = etat.reservations.getOrElse(z, 0)
      val status = if (verrou != 0) f"[VOIE $verrou%02d]" else "[LIBRE  ]"
      print(s"Z$z: $status  ")
    }
    println("\n" + "-" * 53)

    for (i <- 1 to 12) {
      val nb = etat.filesAttente.getOrElse(i, 0)
      val voitures = ">" * nb
      print(f"Voie $i%02d: $voitures%-10s | ")
      if (i % 4 == 0) println()
    }
    println("=====================================================")
    println(">> Stratégie : Pré-réservation complète du trajet")
  }
}