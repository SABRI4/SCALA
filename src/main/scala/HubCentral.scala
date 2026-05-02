import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object HubCentral {

  sealed trait HubCommand
  case class DemandeTrajet(voieId: Int, trajet: List[String], nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class AvancerSequence(voieId: Int, zoneQuittee: String, zoneEntree: String, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassageTotal(voieId: Int, derniereZone: String, nbVehicules: Int) extends HubCommand
  case class PongPanne(voieId: Int) extends HubCommand

  case class EtatCarrefour(
    reservations: Map[String, Int], 
    filesAttente: Map[Int, Int],
    timestamps: Map[String, Long],
    alerteOrange: Boolean = false
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
    var passageUrgence = false
    val resVerifiees = etat.reservations.map { case (zone, occupant) =>
      val debut = etat.timestamps.getOrElse(zone, 0L)
      
      if (occupant > 0 && (maintenant - debut > 30000)) {
        val repondAuPing = scala.util.Random.nextInt(10) > 3

        if (!repondAuPing) {
          println(s"!!! CRITIQUE : Voie $occupant ne répond plus. CONFISCATION ZONES !!!")
          passageUrgence = true
          zone -> -1
        } else {
          println(s"ALERTE : Voie $occupant lente. Libération forcée de $zone.")
          zone -> 0 // On libère simplement
        }
      } else zone -> occupant
    }

    // Si urgence, on verrouille tout  le carrefour
    val nouvelEtatBase = if (passageUrgence || etat.alerteOrange) {
      etat.copy(reservations = resVerifiees.map(_._1 -> -1), alerteOrange = true)
    } else {
      etat.copy(reservations = resVerifiees)
    }

    val finalEtat = message match {
      case PongPanne(id) => 
        println(s"Info : Voie $id a confirmé être en vie.")
        nouvelEtatBase

      case DemandeTrajet(id, trajet, nb, replyTo) =>
        val filesMaj = nouvelEtatBase.filesAttente + (id -> nb)
        // On refuse tout si alerte orange ou zones occupées
        val possible = !nouvelEtatBase.alerteOrange && trajet.forall(z => nouvelEtatBase.reservations.getOrElse(z, 0) == 0)
        
        if (possible) {
          val nouvellesRes = nouvelEtatBase.reservations ++ trajet.map(_ -> id)
          val nouveauxTimes = nouvelEtatBase.timestamps ++ trajet.map(_ -> maintenant)
          replyTo ! CapteurVoie.FeuPasseAuVert
          nouvelEtatBase.copy(reservations = nouvellesRes, filesAttente = filesMaj, timestamps = nouveauxTimes)
        } else {
          nouvelEtatBase.copy(filesAttente = filesMaj)
        }

      case AvancerSequence(id, quittee, entree, replyTo) =>
        val nouvellesRes = nouvelEtatBase.reservations + (quittee -> 0) + (entree -> id)
        val nouveauxTimes = nouvelEtatBase.timestamps + (entree -> maintenant)
        replyTo ! CapteurVoie.FeuPasseAuVert
        nouvelEtatBase.copy(reservations = nouvellesRes, timestamps = nouveauxTimes)

      case FinPassageTotal(id, derniere, nb) =>
        nouvelEtatBase.copy(reservations = nouvelEtatBase.reservations + (derniere -> 0), filesAttente = nouvelEtatBase.filesAttente + (id -> nb))
    }

    afficherCarrefour(finalEtat)
    gestionnaire(finalEtat)
  }

 private def afficherCarrefour(etat: EtatCarrefour): Unit = {
    print("\u001b[2J\u001b[H") // Nettoyage de l'écran

    val vert = "\u001b[32m"; val rouge = "\u001b[31m"; val orange = "\u001b[33m"
    val bleu = "\u001b[34m"; val cyan = "\u001b[36m"; val reset = "\u001b[0m"
    val gras = "\u001b[1m"

    val width = 76 // Largeur fixe du tableau pour un alignement parfait

    // Fonction magique pour aligner la bordure droite sans être cassé par les codes couleurs ANSI
    def padRight(s: String): String = {
      val lengthWithoutColors = s.replaceAll("\u001b\\[[;\\d]*m", "").length
      s + " " * math.max(0, width - lengthWithoutColors)
    }

    println(s"$bleu+${"-" * width}+$reset")
    println(s"$bleu|$reset" + padRight(s"$gras$cyan   CARREFOUR INTELLIGENT  $reset") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    // --- LÉGENDE POUR LE JURY / PROF ---
    println(s"$bleu|$reset" + padRight(s" $gras[ LEGENDE DE LECTURE ]$reset") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Feux       : [V] Vert (Passe) | [R] Rouge (Attend) | [O] Panne/Verrou") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Directions : D = Droite | DR = Tout Droit | G = Gauche") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Trafic     : '>' = 1 Voiture en attente (Max 5 affichees)") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    // --- ZONES CENTRALES ---
    val nomsZones = Map("1" -> "NO", "2" -> "NE", "3" -> "SO", "4" -> "SE")
    val zonesStr = List("2", "1", "3", "4").map { id =>
      val v = etat.reservations.getOrElse(id, 0)
      val (color, label) = if (v == -1) (orange, "BLOQUE") else if (v > 0) (rouge, f"V$v%02d   ") else (vert, "LIBRE ")
      f"[$gras${nomsZones(id)}$reset:$color$label$reset]"
    }.mkString("   ")
    
    println(s"$bleu|$reset" + padRight(f" $gras[ ETAT DES ZONES CENTRALES ]$reset") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight(f"   $zonesStr") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    // --- VOIES ET TRAFIC ---
    println(s"$bleu|$reset" + padRight(f" $gras[ ETAT DES VOIES ET FILES D'ATTENTE ]$reset") + s"$bleu|$reset")
    val maintenant = System.currentTimeMillis()
    val groupes = List(("NORD ", 1 to 3), ("EST  ", 4 to 6), ("SUD  ", 7 to 9), ("OUEST", 10 to 12))

    groupes.foreach { case (nom, ids) =>
      var ligneVoie = s"   $gras$nom$reset : "
      ids.foreach { id =>
        val dir = id % 3 match { case 1 => "D " ; case 2 => "DR"; case _ => "G " }
        val nb = etat.filesAttente.getOrElse(id, 0)
        val estAuVert = etat.reservations.values.exists(_ == id)
        val aUnProb = etat.reservations.exists { case (z, occ) => occ == id && (maintenant - etat.timestamps.getOrElse(z, 0L) > 30000) }

        val feu = if (etat.alerteOrange || aUnProb) s"$orange[O]$reset" else if (estAuVert) s"$vert[V]$reset" else s"$rouge[R]$reset"
        
        // --- LOGIQUE DE L'AFFICHAGE DU TRAFIC ---
        val nbChevrons = if (nb > 5) 5 else nb
        val chevrons = ">" * nbChevrons
        val compteur = if (nb > 5) f"(+$nb%d)" else ""
        val infoTrafic = s"$chevrons$compteur" // On fusionne les deux ici
        
        // On utilise %-11s pour que l'espace total (chevrons + chiffre) soit toujours de 11 caractères
        ligneVoie += f"v$id%02d$feu$dir:$infoTrafic%-11s "
      }
      println(s"$bleu|$reset" + padRight(ligneVoie) + s"$bleu|$reset")
    }
    
    // --- LOGS D'OCCUPATION DÉTAILLÉS ---
    println(s"$bleu|$reset" + padRight(f" $gras[ EVENEMENTS EN TEMPS REEL ]$reset") + s"$bleu|$reset")
    val resActives = etat.reservations.filter(_._2 != 0)
    
    if (resActives.isEmpty) {
      println(s"$bleu|$reset" + padRight("   -> Aucun vehicule n'est actuellement dans le carrefour.") + s"$bleu|$reset")
    } else {
      resActives.foreach { case (zone, voie) =>
        val duree = (maintenant - etat.timestamps.getOrElse(zone, 0L)) / 1000
        if (voie == -1) {
          println(s"$bleu|$reset" + padRight(f"   -> $orange/!\\ SYSTEME : Zone ${nomsZones(zone)} VERROUILLEE (Panne detectee)$reset") + s"$bleu|$reset")
        } else {
          val alerte = if (duree > 15) s"$orange(Attention: Lent)$reset" else ""
          println(s"$bleu|$reset" + padRight(f"   -> Voiture de la Voie $voie%02d traverse ${nomsZones(zone)} depuis $duree sec $alerte") + s"$bleu|$reset")
        }
      }
    }
    println(s"$bleu+${"-" * width}+$reset")

    if (etat.alerteOrange) {
      println(s"$bleu|$reset" + padRight(f" $orange$gras/!\\ URGENCE : MODE SECURITE ACTIVE - TOUT EST BLOQUE /!\\$reset") + s"$bleu|$reset")
    } else {
      println(s"$bleu|$reset" + padRight(f" $cyan>> Strategie Active : Reservation Atomique (Watchdog 30s)$reset") + s"$bleu|$reset")
    }
    println(s"$bleu+${"-" * width}+$reset")
  }
}