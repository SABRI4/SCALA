import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.duration._

object HubCentral {

  sealed trait HubCommand
  case class DemandeTrajet(voieId: Int, trajet: List[String], nbVehicules: Int, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class AvancerSequence(voieId: Int, zoneQuittee: String, zoneEntree: String, replyTo: ActorRef[CapteurVoie.Command]) extends HubCommand
  case class FinPassageTotal(voieId: Int, derniereZone: String, nbVehicules: Int) extends HubCommand
  case class PongPanne(voieId: Int) extends HubCommand
  case object VerifierSanteCapteurs extends HubCommand
  case class PongSante(voieId: Int) extends HubCommand

case class EtatCarrefour(
  reservations: Map[String, Int], 
  filesAttente: Map[Int, Int],
  timestamps: Map[String, Long],
  derniereReponse: Map[Int, Long], //suivi de la vie des acteurs
  acteurs: Map[Int, ActorRef[CapteurVoie.Command]], 
  alerteOrange: Boolean = false
)

def apply(): Behavior[HubCommand] = Behaviors.setup { context =>
  Behaviors.withTimers { timers =>
    // On lance le check toutes les 10s
    timers.startTimerWithFixedDelay(VerifierSanteCapteurs, 10.seconds)
    
    val etatInitial = EtatCarrefour(
      reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0), 
      filesAttente = (1 to 12).map(_ -> 0).toMap,
      timestamps = Map("1" -> 0L, "2" -> 0L, "3" -> 0L, "4" -> 0L),
      derniereReponse = (1 to 12).map(_ -> System.currentTimeMillis()).toMap,
      acteurs = Map.empty
    )
    gestionnaire(etatInitial)
  }
}

private def gestionnaire(etat: EtatCarrefour): Behavior[HubCommand] = Behaviors.receive { (context, message) =>
    val maintenant = System.currentTimeMillis()

    message match {
      //Surveillance de l'état des capteurs (toutes les 10s ici)

      case VerifierSanteCapteurs =>
        etat.acteurs.values.foreach(_ ! CapteurVoie.PingSante(context.self))
        
        val uneVoieEstMorte = etat.derniereReponse.exists { case (_, last) => 
          (maintenant - last) > 30000 
        }

        if (uneVoieEstMorte && !etat.alerteOrange) {
          val etatUrgence = etat.copy(
            alerteOrange = true, 
            reservations = etat.reservations.map { case (zone, _) => zone -> -1 }
          )
          afficherCarrefour(etatUrgence)
          gestionnaire(etatUrgence)
        } else {
          Behaviors.same
        }

      case PongSante(id) =>
        gestionnaire(etat.copy(derniereReponse = etat.derniereReponse + (id -> maintenant)))

      // 2. Gestion du trafic
      case msgTrafic =>
        val finalEtat = msgTrafic match {
          case DemandeTrajet(id, trajet, nb, replyTo) =>
            val nouveauxActeurs = etat.acteurs + (id -> replyTo)
            val filesMaj = etat.filesAttente + (id -> nb)
            
            // Vérification : zones libres (0) et pas d'alerte
            val possible = !etat.alerteOrange && trajet.forall(z => etat.reservations.getOrElse(z, 0) == 0)
            
            if (possible) {
              val nouvellesRes = etat.reservations ++ trajet.map(_ -> id)
              val nouveauxTimes = etat.timestamps ++ trajet.map(_ -> maintenant)
              replyTo ! CapteurVoie.FeuPasseAuVert
              etat.copy(reservations = nouvellesRes, filesAttente = filesMaj, timestamps = nouveauxTimes, acteurs = nouveauxActeurs)
            } else {
              etat.copy(filesAttente = filesMaj, acteurs = nouveauxActeurs)
            }

          case AvancerSequence(id, quittee, entree, replyTo) =>
            val occupant = etat.reservations.getOrElse(entree, 0)
            
            // On avance si c'est libre (0) ou si c'est notre réservation (id)
            if (occupant == 0 || occupant == id) { 
              val nouvellesRes = etat.reservations + (quittee -> 0) + (entree -> id)
              val nouveauxTimes = etat.timestamps + (entree -> maintenant)
              replyTo ! CapteurVoie.FeuPasseAuVert
              etat.copy(reservations = nouvellesRes, timestamps = nouveauxTimes)
            } else {
              etat
            }

          case FinPassageTotal(id, derniere, nb) =>
            etat.copy(
              reservations = etat.reservations + (derniere -> 0), 
              filesAttente = etat.filesAttente + (id -> nb)
            )

          case _ => etat 
        }

        afficherCarrefour(finalEtat)
        gestionnaire(finalEtat)
    }
  }

private def afficherCarrefour(etat: EtatCarrefour): Unit = {
    print("\u001b[2J\u001b[H") // Nettoyage de l'écran

    val vert = "\u001b[32m"; val rouge = "\u001b[31m"; val orange = "\u001b[33m"
    val bleu = "\u001b[34m"; val cyan = "\u001b[36m"; val reset = "\u001b[0m"
    val gras = "\u001b[1m"

    val width = 76

    def padRight(s: String): String = {
      val lengthWithoutColors = s.replaceAll("\u001b\\[[;\\d]*m", "").length
      s + " " * math.max(0, width - lengthWithoutColors)
    }

    println(s"$bleu+${"-" * width}+$reset")
    println(s"$bleu|$reset" + padRight(s"$gras$cyan   CARREFOUR INTELLIGENT  $reset") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    println(s"$bleu|$reset" + padRight(s" $gras[ LEGENDE DE LECTURE ]$reset") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Feux       : [V] Vert (Passe) | [R] Rouge (Attend) | [O] Panne Système") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Directions : D = Droite | DR = Tout Droit | G = Gauche") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight("   Trafic     : '>' = 1 Voiture en attente (Max 5 affichees)") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    //ZONES CARREFOUR
    val nomsZones = Map("1" -> "NO", "2" -> "NE", "3" -> "SO", "4" -> "SE")
    val zonesStr = List("2", "1", "3", "4").map { id =>
      val v = etat.reservations.getOrElse(id, 0)
      // Ici le orange n'apparaît que si le Hub force le verrouillage (-1)
      val (color, label) = if (v == -1) (orange, "BLOQUE") else if (v > 0) (rouge, f"V$v%02d   ") else (vert, "LIBRE ")
      f"[$gras${nomsZones(id)}$reset:$color$label$reset]"
    }.mkString("   ")
    
    println(s"$bleu|$reset" + padRight(f" $gras[ ETAT DES ZONES CENTRALES ]$reset") + s"$bleu|$reset")
    println(s"$bleu|$reset" + padRight(f"   $zonesStr") + s"$bleu|$reset")
    println(s"$bleu+${"-" * width}+$reset")

    println(s"$bleu|$reset" + padRight(f" $gras[ ETAT DES VOIES ET FILES D'ATTENTE ]$reset") + s"$bleu|$reset")
    val maintenant = System.currentTimeMillis()
    val groupes = List(("NORD ", 1 to 3), ("EST  ", 4 to 6), ("SUD  ", 7 to 9), ("OUEST", 10 to 12))

    groupes.foreach { case (nom, ids) =>
      var ligneVoie = s"   $gras$nom$reset : "
      ids.foreach { id =>
        val dir = id % 3 match { case 1 => "D " ; case 2 => "DR"; case _ => "G " }
        val nb = etat.filesAttente.getOrElse(id, 0)
        val estAuVert = etat.reservations.values.exists(_ == id)

        // MODIFICATION : On a supprimé 'aUnProb'. Le feu n'est orange que si 'alerteOrange' est vrai (panne capteur)
        val feu = if (etat.alerteOrange) s"$orange[O]$reset" else if (estAuVert) s"$vert[V]$reset" else s"$rouge[R]$reset"
      
        val nbChevrons = if (nb > 5) 5 else nb
        val chevrons = ">" * nbChevrons
        val compteur = if (nb > 5) f"(+$nb%d)" else ""
        val infoTrafic = s"$chevrons$compteur" 
      
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
          println(s"$bleu|$reset" + padRight(f"   -> $orange/!\\ SYSTEME : Zone ${nomsZones(zone)} VERROUILLEE$reset") + s"$bleu|$reset")
        } else {
          // MODIFICATION : suppression du texte "(Attention: Lent)"
          println(s"$bleu|$reset" + padRight(f"   -> Voiture de la Voie $voie%02d traverse ${nomsZones(zone)} depuis $duree s") + s"$bleu|$reset")
        }
      }
    }
    println(s"$bleu+${"-" * width}+$reset")

    if (etat.alerteOrange) {
      println(s"$bleu|$reset" + padRight(f" $orange$gras/!\\ PROTOCOLE DE SECURITE : PANNE MATERIELLE /!\\$reset") + s"$bleu|$reset")
      println(s"$bleu|$reset" + padRight("   Les capteurs ne repondent plus. Passage au code de la route.") + s"$bleu|$reset")
    } else {
      println(s"$bleu|$reset" + padRight(f" $cyan>> CARREFOUR INTELLIGENT : ON $reset") + s"$bleu|$reset")
    }
    println(s"$bleu+${"-" * width}+$reset")
  }
}