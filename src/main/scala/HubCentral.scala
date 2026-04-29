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
    // Nettoyage de l'écran
    print("\u001b[2J\u001b[H")
    
    val vert = "\u001b[32m"
    val rouge = "\u001b[31m"
    val reset = "\u001b[0m"
    val bleu = "\u001b[34m"

    println(s"$bleu=====================================================$reset")
    println(s"$bleu        MONITORING CARREFOUR CRITIQUE 2026           $reset")
    println(s"$bleu=====================================================$reset")
    
    // 1. ÉTAT DES ZONES (CENTRE DU CARREFOUR)
    val nomsZones = Map("1" -> "NO", "2" -> "NE", "3" -> "SO", "4" -> "SE")
    print("ZONES CENTRALES : ")
    List("2", "1", "3", "4").foreach { id =>
      val verrou = etat.reservations.getOrElse(id, 0)
      val nom = nomsZones(id)
      val status = if (verrou != 0) f"V$verrou%02d" else "LIBRE"
      val color = if (verrou != 0) rouge else vert
      print(s"[$nom:$color$status$reset]  ")
    }
    println("\n" + "-" * 53)

    //DISPOSITION GÉOGRAPHIQUE DES VOIES
    val groupes = List(
      ("NORD (v01-v03)", List(1, 2, 3)),
      ("SUD  (v04-v06)", List(4, 5, 6)),
      ("EST  (v07-v09)", List(7, 8, 9)),
      ("OUEST(v10-v12)", List(10, 11, 12))
    )

    groupes.foreach { case (nom, ids) =>
      print(f"$nom%-15s : ")
      ids.foreach { id =>
        val nb = etat.filesAttente.getOrElse(id, 0)
        val estAuVert = etat.reservations.values.exists(_ == id)
        val feu = if (estAuVert) s"$vert[V]$reset" else s"$rouge[R]$reset"
        val voitures = ">" * nb
        print(f"v$id%02d$feu:$voitures%-6s  ")
      }
      println()
    }

    println("-" * 53)

    // 3. LOG DE RÉSERVATION (POUR COMPRENDRE QUI BLOQUE QUI)
    println("RESERVATIONS ACTIVES :")
    val resActives = etat.reservations.filter(_._2 != 0)
    if (resActives.isEmpty) println("Aucun vehicule engager.")
    else {
      resActives.foreach { case (zone, voie) =>
        println(s"La Voie $voie occupe/reserve la Zone ${nomsZones(zone)}")
      }
    }

    println(s"$bleu=====================================================$reset")
    println(">> [V] = Vert | [R] = Rouge | Stratégie : Onde Verte")
  }
}