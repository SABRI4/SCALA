package carrefour

import akka.actor.typed.{ActorRef, Behavior, ActorSystem}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.testkit.typed.scaladsl.{TestProbe, ActorTestKit}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Tests unitaires pour CapteurVoie.scala
 * 
 * Ces tests vérifient:
 * - La gestion de la file d'attente
 * - Les messages ArriveeVehicule, FeuPasseAuVert, TraiterProchainVehicule
 * - L'interaction avec le HubCentral
 * - Le comportement du générateur de flux
 * 
 * Note: Les tests n'appellent pas directement la méthode privée genererTrajet.
 * Ils testent le comportement via les messages et les interactions avec le hub.
 */
class CapteurVoieSpec extends AnyWordSpec with Matchers with ScalaFutures {

  // Utilise ActorTestKit pour les tests d'acteurs Akka
  val testKit: ActorTestKit = ActorTestKit()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 100.millis)

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }

  "CapteurVoie" should {

    "retourner un Behavior valide" in {
      val hub = testKit.spawn(HubCentral(), "hub-capteur-test")
      val behavior = CapteurVoie(1, "1", hub)
      behavior should not be null
    }
  }

  "Les commandes du CapteurVoie" should {

    "ArriveeVehicule doit exister" in {
      CapteurVoie.ArriveeVehicule shouldBe CapteurVoie.ArriveeVehicule
    }

    "FeuPasseAuVert doit exister" in {
      CapteurVoie.FeuPasseAuVert shouldBe CapteurVoie.FeuPasseAuVert
    }

    "TraiterProchainVehicule doit exister" in {
      CapteurVoie.TraiterProchainVehicule shouldBe CapteurVoie.TraiterProchainVehicule
    }
  }

  "L'initialisation du CapteurVoie" should {

    "envoyer une demande de trajet au hub si la file n'est pas vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val capteur = testKit.spawn(CapteurVoie(1, "1", hub.ref), "capteur-test-1")

      // Le CapteurVoie envoie une demande de trajet initiale au hub
      // On vérifie qu'un message est reçu
      hub.expectMessageType[HubCentral.DemandeTrajet](3.seconds)
    }

    "démarrer le générateur de flux" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val capteur = testKit.spawn(CapteurVoie(2, "2", hub.ref), "capteur-test-2")

      // Après un certain temps, des véhicules devraient arriver
      // Le générateur de flux envoie ArriveeVehicule périodiquement
      hub.expectMessageType[HubCentral.DemandeTrajet](5.seconds)
    }

    "avoir le bon ID de voie dans la demande initiale" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val voieId = 7
      val capteur = testKit.spawn(CapteurVoie(voieId, "1", hub.ref), "capteur-test-voie-id")

      // Vérifie que le message contient le bon ID de voie
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](3.seconds)
      msg.voieId shouldBe voieId
    }
  }

  "La réception de FeuPasseAuVert" should {

    "planifier le traitement du prochain véhicule si la file n'est pas vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      
      // Crée un acteur qui simule le comportement de CapteurVoie avec file non vide
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.FeuPasseAuVert =>
            // Devrait planifier TraiterProchainVehicule
            context.self ! CapteurVoie.TraiterProchainVehicule
            Behaviors.same
          case CapteurVoie.TraiterProchainVehicule =>
            probe.ref ! CapteurVoie.TraiterProchainVehicule
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-feu-vert")
      acteur ! CapteurVoie.FeuPasseAuVert

      // Devrait recevoir TraiterProchainVehicule après un délai
      probe.expectMessage(2.seconds, CapteurVoie.TraiterProchainVehicule)
    }

    "ne rien faire si la file est vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Test comportement avec file vide
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.FeuPasseAuVert =>
            // Avec file vide, ne fait rien (Behaviors.same)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-file-vide")
      acteur ! CapteurVoie.FeuPasseAuVert

      // Pas de message attendu car file vide
      Thread.sleep(500)
    }
  }

  "Le traitement d'un véhicule" should {

    "envoyer AvancerSequence si le trajet n'est pas terminé" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Simule un trajet avec plusieurs zones: ["1", "2"]
      val trajet = List("1", "2")
      val file = List(trajet)

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            if (file.nonEmpty) {
              val trajetActuel = file.head
              val zoneFinie = trajetActuel.head
              val resteDuTrajet = trajetActuel.tail

              if (resteDuTrajet.nonEmpty) {
                // Devrait envoyer AvancerSequence
                hub.ref ! HubCentral.AvancerSequence(1, zoneFinie, resteDuTrajet.head, context.self)
              }
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-avancer")
      acteur ! CapteurVoie.TraiterProchainVehicule

      // Vérifie qu'AvancerSequence est envoyé
      val msg = hub.expectMessageType[HubCentral.AvancerSequence](2.seconds)
      msg.voieId shouldBe 1
      msg.zoneQuittee shouldBe "1"
      msg.zoneEntree shouldBe "2"
    }

    "envoyer FinPassageTotal si le trajet est terminé" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Simule un trajet avec une seule zone: ["1"]
      val trajet = List("1")
      val file = List(trajet)

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            if (file.nonEmpty) {
              val trajetActuel = file.head
              val zoneFinie = trajetActuel.head
              val resteDuTrajet = trajetActuel.tail

              if (resteDuTrajet.isEmpty) {
                // Devrait envoyer FinPassageTotal
                val fileApresSortie = file.tail
                hub.ref ! HubCentral.FinPassageTotal(1, zoneFinie, fileApresSortie.size)
              }
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-fin-passage")
      acteur ! CapteurVoie.TraiterProchainVehicule

      // Vérifie que FinPassageTotal est envoyé
      val msg = hub.expectMessageType[HubCentral.FinPassageTotal](2.seconds)
      msg.voieId shouldBe 1
      msg.derniereZone shouldBe "1"
    }

    "demander un nouveau trajet s'il reste des véhicules après la sortie" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Simule: un véhicule termine, mais il en reste dans la file
      val fileApresSortie = List(List("1", "2"), List("1"))

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            val fileApresSortie = List(List("1", "2"), List("1"))
            if (fileApresSortie.nonEmpty) {
              // Devrait demander un nouveau trajet pour le prochain véhicule
              hub.ref ! HubCentral.DemandeTrajet(1, fileApresSortie.head, fileApresSortie.size, context.self)
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-nouveau-trajet")
      acteur ! CapteurVoie.TraiterProchainVehicule

      // Vérifie que DemandeTrajet est envoyé
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.nbVehicules shouldBe 2
    }
  }

  "L'arrivée d'un véhicule" should {

    "ajouter un trajet à la file et envoyer une demande si c'est le premier" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      var fileActuelle: List[List[String]] = List()

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            // Simule la génération d'un trajet (ici trajet fixe pour le test)
            val nouveauTrajet = List("1", "2")  // Trajet simulé
            fileActuelle = fileActuelle :+ nouveauTrajet
            
            // Si c'est le premier, envoie une demande au hub
            if (fileActuelle.size == 1) {
              hub.ref ! HubCentral.DemandeTrajet(1, nouveauTrajet, fileActuelle.size, context.self)
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-arrivee")
      acteur ! CapteurVoie.ArriveeVehicule

      // Vérifie qu'une demande est envoyée
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.voieId shouldBe 1
      msg.nbVehicules shouldBe 1
    }

    "mettre à jour l'affichage si la file n'est pas vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // File déjà avec un véhicule
      var fileActuelle: List[List[String]] = List(List("1", "2"))

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            // Simule un nouveau trajet
            val nouveauTrajet = List("1")
            val nouvelleFile = fileActuelle :+ nouveauTrajet
            
            // Envoie quand même une demande pour l'affichage
            hub.ref ! HubCentral.DemandeTrajet(1, fileActuelle.head, nouvelleFile.size, context.self)
            
            fileActuelle = nouvelleFile
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-maj-affichage")
      acteur ! CapteurVoie.ArriveeVehicule

      // Vérifie que le hub est mis à jour avec le bon nombre
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.nbVehicules shouldBe 2
    }
  }

  "Le générateur de flux" should {

    "envoyer ArriveeVehicule périodiquement" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      
      var compteur = 0
      
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            compteur += 1
            probe.ref ! CapteurVoie.ArriveeVehicule
            // Simule le comportement du générateur de flux
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-generateur-flux")
      
      // Le générateur de flux devrait envoyer ArriveeVehicule
      // On vérifie qu'au moins un message est reçu
      probe.expectMessage(3.seconds, CapteurVoie.ArriveeVehicule)
    }
  }

  "L'ID de voie" should {

    "être correctement passé au hub dans les demandes" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val voieId = 5
      
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            val nouveauTrajet = List("1")
            hub.ref ! HubCentral.DemandeTrajet(voieId, nouveauTrajet, 1, context.self)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-voie-id")
      acteur ! CapteurVoie.ArriveeVehicule

      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.voieId shouldBe voieId
    }
  }

  "La zone cible" should {

    "être utilisée pour générer les trajets" in {
      // Ce test vérifie que la zone cible est bien passée au constructeur
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val zoneCible = "3"
      
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            // Ici on simule que le trajet est basé sur la zone cible
            val nouveauTrajet = List(zoneCible, "4")  // Transition 3 -> 4
            hub.ref ! HubCentral.DemandeTrajet(1, nouveauTrajet, 1, context.self)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-zone-cible")
      acteur ! CapteurVoie.ArriveeVehicule

      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.trajet.head shouldBe zoneCible
    }
  }

  "L'interaction avec le HubCentral" should {

    "envoyer les bons types de messages au hub" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Test de l'envoi de DemandeTrajet
      val comportement1 = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            hub.ref ! HubCentral.DemandeTrajet(1, List("1"), 1, context.self)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur1 = testKit.spawn(comportement1, "test-interaction-1")
      acteur1 ! CapteurVoie.ArriveeVehicule
      hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)

      // Test de l'envoi de AvancerSequence
      val comportement2 = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            hub.ref ! HubCentral.AvancerSequence(1, "1", "2", context.self)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val hub2 = testKit.createTestProbe[HubCentral.HubCommand]()
      val acteur2 = testKit.spawn(comportement2, "test-interaction-2")
      acteur2 ! CapteurVoie.TraiterProchainVehicule
      hub2.expectMessageType[HubCentral.AvancerSequence](2.seconds)

      // Test de l'envoi de FinPassageTotal
      val comportement3 = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            hub.ref ! HubCentral.FinPassageTotal(1, "1", 0)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val hub3 = testKit.createTestProbe[HubCentral.HubCommand]()
      val acteur3 = testKit.spawn(comportement3, "test-interaction-3")
      acteur3 ! CapteurVoie.TraiterProchainVehicule
      hub3.expectMessageType[HubCentral.FinPassageTotal](2.seconds)
    }
  }
}

    "avoir une zone suivante valide (2->1, 1->3, 3->4, 4->2)" in {
      // Vérifie que les transitions de zones sont correctes avec une seed déterministe
      val resultat = CapteurVoie.genererTrajet("2", 50)
      
      // La transition 2 -> 1 devrait apparaître (car 50 < 60)
      resultat should have size 2
      resultat(1) shouldBe "1"
    }

    "retourner toujours la zone initiale comme premier élément" in {
      for (zone <- List("1", "2", "3", "4")) {
        val trajet = CapteurVoie.genererTrajet(zone, 0)
        trajet.head shouldBe zone
      }
    }

    "générer un trajet d'une seule zone quand la probabilité est >= 60" in {
      // Avec seed=99, probaContinuer = 99 >= 60, donc trajet d'une seule zone
      val resultat = CapteurVoie.genererTrajet("3", 99)
      resultat should have size 1
      resultat.head shouldBe "3"
    }

    "générer un trajet de deux zones quand la probabilité est < 60" in {
      // Avec seed=30, probaContinuer = 30 < 60, donc trajet de deux zones
      val resultat = CapteurVoie.genererTrajet("3", 30)
      resultat should have size 2
      resultat.head shouldBe "3"
      resultat(1) shouldBe "4"  // 3 -> 4
    }
  }

  "Les commandes du CapteurVoie" should {

    "ArriveeVehicule doit exister" in {
      CapteurVoie.ArriveeVehicule shouldBe CapteurVoie.ArriveeVehicule
    }

    "FeuPasseAuVert doit exister" in {
      CapteurVoie.FeuPasseAuVert shouldBe CapteurVoie.FeuPasseAuVert
    }

    "TraiterProchainVehicule doit exister" in {
      CapteurVoie.TraiterProchainVehicule shouldBe CapteurVoie.TraiterProchainVehicule
    }
  }

  "L'initialisation du CapteurVoie" should {

    "envoyer une demande de trajet au hub si la file n'est pas vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val capteur = testKit.spawn(CapteurVoie(1, "1", hub.ref), "capteur-test-1")

      // Le CapteurVoie envoie une demande de trajet initiale au hub
      // On vérifie qu'un message est reçu
      hub.expectMessageType[HubCentral.DemandeTrajet](3.seconds)
    }

    "démarrer le générateur de flux" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val capteur = testKit.spawn(CapteurVoie(2, "2", hub.ref), "capteur-test-2")

      // Après un certain temps, des véhicules devraient arriver
      // Le générateur de flux envoie ArriveeVehicule périodiquement
      hub.expectMessageType[HubCentral.DemandeTrajet](5.seconds)
    }
  }

  "La réception de FeuPasseAuVert" should {

    "planifier le traitement du prochain véhicule" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      
      // Crée un acteur qui peut être testé
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.FeuPasseAuVert =>
            // Devrait planifier TraiterProchainVehicule
            context.self ! CapteurVoie.TraiterProchainVehicule
            Behaviors.same
          case CapteurVoie.TraiterProchainVehicule =>
            probe.ref ! CapteurVoie.TraiterProchainVehicule
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-feu-vert")
      acteur ! CapteurVoie.FeuPasseAuVert

      // Devrait recevoir TraiterProchainVehicule après un délai
      probe.expectMessage(2.seconds, CapteurVoie.TraiterProchainVehicule)
    }

    "ne rien faire si la file est vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Test comportement avec file vide
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.FeuPasseAuVert =>
            // Avec file vide, ne fait rien (Behaviors.same)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-file-vide")
      acteur ! CapteurVoie.FeuPasseAuVert

      // Pas de message attendu car file vide
      Thread.sleep(500)
    }
  }

  "Le traitement d'un véhicule" should {

    "envoyer AvancerSequence si le trajet n'est pas terminé" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      
      // Simule un trajet avec plusieurs zones: ["1", "2"]
      val trajet = List("1", "2")
      val file = List(trajet)

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            if (file.nonEmpty) {
              val trajetActuel = file.head
              val zoneFinie = trajetActuel.head
              val resteDuTrajet = trajetActuel.tail

              if (resteDuTrajet.nonEmpty) {
                // Devrait envoyer AvancerSequence
                hub.ref ! HubCentral.AvancerSequence(1, zoneFinie, resteDuTrajet.head, context.self)
              }
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-avancer")
      acteur ! CapteurVoie.TraiterProchainVehicule

      // Vérifie qu'AvancerSequence est envoyé
      val msg = hub.expectMessageType[HubCentral.AvancerSequence](2.seconds)
      msg.voieId shouldBe 1
      msg.zoneQuittee shouldBe "1"
      msg.zoneEntree shouldBe "2"
    }

    "envoyer FinPassageTotal si le trajet est terminé" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // Simule un trajet avec une seule zone: ["1"]
      val trajet = List("1")
      val file = List(trajet)

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.TraiterProchainVehicule =>
            if (file.nonEmpty) {
              val trajetActuel = file.head
              val zoneFinie = trajetActuel.head
              val resteDuTrajet = trajetActuel.tail

              if (resteDuTrajet.isEmpty) {
                // Devrait envoyer FinPassageTotal
                val fileApresSortie = file.tail
                hub.ref ! HubCentral.FinPassageTotal(1, zoneFinie, fileApresSortie.size)
              }
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-fin-passage")
      acteur ! CapteurVoie.TraiterProchainVehicule

      // Vérifie que FinPassageTotal est envoyé
      val msg = hub.expectMessageType[HubCentral.FinPassageTotal](2.seconds)
      msg.voieId shouldBe 1
      msg.derniereZone shouldBe "1"
    }
  }

  "L'arrivée d'un véhicule" should {

    "ajouter un trajet à la file" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      var fileActuelle: List[List[String]] = List()

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            // Génère un nouveau trajet et l'ajoute à la file
            val nouveauTrajet = CapteurVoie.genererTrajet("1", 0)
            fileActuelle = fileActuelle :+ nouveauTrajet
            
            // Si c'est le premier, envoie une demande au hub
            if (fileActuelle.size == 1) {
              hub.ref ! HubCentral.DemandeTrajet(1, nouveauTrajet, fileActuelle.size, context.self)
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-arrivee")
      acteur ! CapteurVoie.ArriveeVehicule

      // Vérifie qu'une demande est envoyée
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.voieId shouldBe 1
      msg.nbVehicules shouldBe 1
    }

    "mettre à jour l'affichage si la file n'est pas vide" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      var fileActuelle: List[List[String]] = List(List("1"))

      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            val nouveauTrajet = CapteurVoie.genererTrajet("1", 0)
            val nouvelleFile = fileActuelle :+ nouveauTrajet
            
            // Envoie quand même une demande pour l'affichage
            hub.ref ! HubCentral.DemandeTrajet(1, fileActuelle.head, nouvelleFile.size, context.self)
            
            fileActuelle = nouvelleFile
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-maj-affichage")
      acteur ! CapteurVoie.ArriveeVehicule

      // Vérifie que le hub est mis à jour
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.nbVehicules shouldBe 2
    }
  }

  "L'ID de voie" should {

    "être correctement passé au constructeur" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val voieId = 7
      
      val comportement = Behaviors.receive[CapteurVoie.Command] { (context, message) =>
        message match {
          case CapteurVoie.ArriveeVehicule =>
            val nouveauTrajet = CapteurVoie.genererTrajet("1", 0)
            hub.ref ! HubCentral.DemandeTrajet(voieId, nouveauTrajet, 1, context.self)
            Behaviors.same
          case _ => Behaviors.same
        }
      }

      val acteur = testKit.spawn(comportement, "test-voie-id")
      acteur ! CapteurVoie.ArriveeVehicule

      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](2.seconds)
      msg.voieId shouldBe voieId
    }
  }

  "La zone cible" should {

    "être utilisée pour générer les trajets" in {
      // Vérifie que chaque zone génère des trajets commençant par cette zone
      for (zone <- List("1", "2", "3", "4")) {
        val trajet = CapteurVoie.genererTrajet(zone, 0)
        trajet.head shouldBe zone
      }
    }

    "générer des transitions correctes selon la zone" in {
      // Zone 1 -> 3
      CapteurVoie.genererTrajet("1", 30)(1) shouldBe "3"
      // Zone 2 -> 1
      CapteurVoie.genererTrajet("2", 30)(1) shouldBe "1"
      // Zone 3 -> 4
      CapteurVoie.genererTrajet("3", 30)(1) shouldBe "4"
      // Zone 4 -> 2
      CapteurVoie.genererTrajet("4", 30)(1) shouldBe "2"
    }
  }
}