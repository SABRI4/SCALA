import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object HubCentral {
  // Définition des commandes que HubCentral peut recevoir des capteurs de voie

  sealed trait HubCommand
  case class DemandeAcces(voieId: Int, zoneCible: String, nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassage(voieId: Int, zoneCible: String, nbVehicules: Int) extends HubCommand

  // L'état contient l'occupation des zones ET le nombre de voitures par voie
  case class EtatCarrefour(
    occupations: Map[String, Boolean],
    filesAttente: Map[Int, Int]
  )

  def apply(): Behavior[HubCommand] = {
    val etatInitial = EtatCarrefour(
      Map("NE" -> false, "NO" -> false, "SE" -> false, "SO" -> false),
      (1 to 16).map(_ -> 0).toMap
    )
    gestionnaire(etatInitial)
  }

  // Là ou on va gérer la logique des commandes recues et mettre à jour l'état du carrefour en conséquence (par Behavior.receive)
  private def gestionnaire(etat: EtatCarrefour): Behavior[HubCommand] = Behaviors.receive { (context, message) =>
    val nouvelEtat = message match {
      case DemandeAcces(id, zone, nb, replyTo) =>
        val estOccupee = etat.occupations.getOrElse(zone, true)
        val filesMaj = etat.filesAttente + (id -> nb)

        if (!estOccupee) {
          replyTo ! CapteurVoie.FeuPasseAuVert
          etat.copy(occupations = etat.occupations + (zone -> true), filesAttente = filesMaj)
        } else {
          // On met juste à jour le nombre de voitures dans la file pour l'affichage
          etat.copy(filesAttente = filesMaj)
        }

      case FinPassage(id, zone, nb) =>
        etat.copy(
          occupations = etat.occupations + (zone -> false),
          filesAttente = etat.filesAttente + (id -> nb)
        )
    }

    // On rafraîchit le terminal à chaque message
    afficherCarrefour(nouvelEtat)
    // Pour que sa boucle en continue avec le nouvel état à jour
    gestionnaire(nouvelEtat)
  }

  private def afficherCarrefour(etat: EtatCarrefour): Unit = {
  // Efface l'écran proprement
    print("\u001b[2J\u001b[H")
    
    println("=====================================================")
    println("       MONITORING CARREFOUR CRITIQUE 2026           ")
    println("=====================================================")
    
  
    print("ZONES : ")
    val zonesOrdre = List("NE", "NO", "SE", "SO")
    zonesOrdre.foreach { z =>
      val status = if (etat.occupations(z)) "[OCCUPE]" else "[LIBRE ]"
      print(s"$z: $status   ")
    }
    println("\n" + "-" * 53)

    // Affichage des Voies (V = voiture)
    for (i <- 1 to 12) {
      val nb = etat.filesAttente.getOrElse(i, 0)
      val voitures = ">" * nb
      print(f"Voie $i%02d: $voitures%-10s | ")
      if (i % 4 == 0) println()
    }
    
    println("=====================================================")
    println(">> Appuyez sur Ctrl+C pour arrêter la simulation")
  }
}