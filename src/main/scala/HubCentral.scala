import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object HubCentral {

  sealed trait HubCommand
  case class DemandeTrajet(voieId: Int, trajet: List[String], nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class AvancerSequence(voieId: Int, zoneQuittee: String, zoneEntree: String, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassageTotal(voieId: Int, derniereZone: String, nbVehicules: Int) extends HubCommand

  case class EtatCarrefour(
    reservations: Map[String, Int], 
    filesAttente: Map[Int, Int],
    timestamps: Map[String, Long] // Stocke l'heure du verrouillage pour détecter les pannes
  )

  def apply(): Behavior[HubCommand] = {
    val etatInitial = EtatCarrefour(
      reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0), 
      filesAttente = (1 to 12).map(_ -> 0).toMap,
      timestamps = Map("1" -> 0L, "2" -> 0L, "3" -> 0L, "4" -> 0L)
    )
    gestionnaire(etatInitial)
  }

  private def gestionnaire(etat: EtatCarrefour): Behavior[HubCommand] = Behaviors.receive { (context, message) =>
    val maintenant = System.currentTimeMillis()

    // Handler des pannes
    val resNettoyees = etat.reservations.map { case (zone, occupant) =>
      val debut = etat.timestamps.getOrElse(zone, 0L)
      if (occupant != 0 && (maintenant - debut > 15000)) {
        // La zone est libérée d'office, le capteur est considéré en panne ou trop lent
        zone -> 0 
      } else {
        zone -> occupant
      }
    }
    
    // On met à jour l'état avec les zones potentiellement libérées par le Watchdog
    val etatBase = etat.copy(reservations = resNettoyees)

    val nouvelEtat = message match {
      case DemandeTrajet(id, trajet, nb, replyTo) =>
        val zonesLibres = trajet.forall(z => etatBase.reservations.getOrElse(z, 0) == 0)
        val filesMaj = etatBase.filesAttente + (id -> nb)

        if (zonesLibres) {
          val nouvellesRes = etatBase.reservations ++ trajet.map(_ -> id)
          val nouveauxTimes = etatBase.timestamps ++ trajet.map(_ -> maintenant)
          replyTo ! CapteurVoie.FeuPasseAuVert
          etatBase.copy(reservations = nouvellesRes, filesAttente = filesMaj, timestamps = nouveauxTimes)
        } else {
          etatBase.copy(filesAttente = filesMaj)
        }

      case AvancerSequence(id, quittee, entree, replyTo) =>
        // Libère la zone quittée et met à jour le chrono pour la nouvelle zone
        val nouvellesRes = etatBase.reservations + (quittee -> 0)
        val nouveauxTimes = etatBase.timestamps + (entree -> maintenant)
        replyTo ! CapteurVoie.FeuPasseAuVert
        etatBase.copy(reservations = nouvellesRes, timestamps = nouveauxTimes)

      case FinPassageTotal(id, derniere, nb) =>
        val nouvellesRes = etatBase.reservations + (derniere -> 0)
        val filesMaj = etatBase.filesAttente + (id -> nb)
        etatBase.copy(reservations = nouvellesRes, filesAttente = filesMaj)
    }

    afficherCarrefour(nouvelEtat)
    gestionnaire(nouvelEtat)
  }

  private def afficherCarrefour(etat: EtatCarrefour): Unit = {
    print("\u001b[2J\u001b[H")
    
    val vert = "\u001b[32m"
    val rouge = "\u001b[31m"
    val reset = "\u001b[0m"
    val bleu = "\u001b[34m"

    println(s"$bleu=====================================================$reset")
    println(s"$bleu          MONITORING CARREFOUR CRITIQUE 2026         $reset")
    println(s"$bleu=====================================================$reset")
    
    val nomsZones = Map("1" -> "NO", "2" -> "NE", "3" -> "SO", "4" -> "SE")
    print("ZONES CENTRALES : ")
    List("2", "1", "3", "4").foreach { id =>
      val verrou = etat.reservations.getOrElse(id, 0)
      val status = if (verrou != 0) f"V$verrou%02d" else "LIBRE"
      val color = if (verrou != 0) rouge else vert
      print(s"[${nomsZones(id)}:$color$status$reset]  ")
    }
    println("\n" + "-" * 53)

    val groupes = List(
      ("NORD (v01-v03)", List(1, 2, 3)),
      ("EST  (v04-v06)", List(4, 5, 6)),
      ("SUD  (v07-v09)", List(7, 8, 9)),
      ("OUEST(v10-v12)", List(10, 11, 12))
    )

    groupes.foreach { case (nom, ids) =>
      print(f"$nom%-15s : ")
      ids.foreach { id =>
        val dir = id % 3 match {
          case 1 => "D"
          case 2 => "DR"
          case 0 => "G"
        }
        val nb = etat.filesAttente.getOrElse(id, 0)
        val estAuVert = etat.reservations.values.exists(_ == id)
        val feu = if (estAuVert) s"$vert[V]$reset" else s"$rouge[R]$reset"
        val voitures = ">" * nb
        print(f"v$id%02d[$dir]$feu:$voitures%-6s  ")
      }
      println()
    }

    println("-" * 53)

    println("RESERVATIONS ACTIVES :")
    val resActives = etat.reservations.filter(_._2 != 0)
    if (resActives.isEmpty) println("Aucun vehicule engage.")
    else {
      resActives.foreach { case (zone, voie) =>
        val duree = (System.currentTimeMillis() - etat.timestamps.getOrElse(zone, 0L)) / 1000
        println(s"La Voie $voie occupe la zone ${nomsZones(zone)} ($duree s)")
      }
    }

    println(s"$bleu=====================================================$reset")
    println(s">> SECURITE : Watchdog 15s actif | Strategie: Onde Verte")
  }
}