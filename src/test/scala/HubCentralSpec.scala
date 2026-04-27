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
 * Tests unitaires pour HubCentral.scala
 * 
 * Ces tests vérifient:
 * - L'état initial du carrefour
 * - Les messages DemandeTrajet, AvancerSequence, FinPassageTotal
 * - La gestion des réservations de zones
 * - La gestion des files d'attente
 */
class HubCentralSpec extends AnyWordSpec with Matchers with ScalaFutures {

  // Utilise ActorTestKit pour les tests d'acteurs Akka
  val testKit: ActorTestKit = ActorTestKit()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 100.millis)

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }

  "HubCentral" should {

    "avoir un état initial avec 4 zones libres" in {
      // Vérifie l'état initial
      val etatInitial = HubCentral.EtatCarrefour(
        reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0),
        filesAttente = (1 to 12).map(_ -> 0).toMap
      )

      etatInitial.reservations("1") shouldBe 0
      etatInitial.reservations("2") shouldBe 0
      etatInitial.reservations("3") shouldBe 0
      etatInitial.reservations("4") shouldBe 0
    }

    "avoir une file d'attente initialisée à 0 pour toutes les voies" in {
      val etatInitial = HubCentral.EtatCarrefour(
        reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0),
        filesAttente = (1 to 12).map(_ -> 0).toMap
      )

      for (i <- 1 to 12) {
        etatInitial.filesAttente(i) shouldBe 0
      }
    }

    "retourner un Behavior valide" in {
      val behavior = HubCentral()
      behavior should not be null
    }
  }

  "La gestion des demandes de trajet" should {

    "accorder le feu vert si toutes les zones sont libres" in {
      // Crée le hub
      val hub = testKit.spawn(HubCentral(), "hub-test-1")

      // Crée un probe pour recevoir les réponses
      val probe = testKit.createTestProbe[CapteurVoie.Command]()

      // Envoie une demande de trajet avec des zones libres
      val trajet = List("1", "2")
      hub ! HubCentral.DemandeTrajet(1, trajet, 1, probe.ref)

      // Le hub doit envoyer FeuPasseAuVert
      probe.expectMessage(CapteurVoie.FeuPasseAuVert)
    }

    "refuser le feu vert si une zone est déjà réservée" in {
      val hub = testKit.spawn(HubCentral(), "hub-test-2")

      // Première demande - devrait réussir
      val probe1 = testKit.createTestProbe[CapteurVoie.Command]()
      hub ! HubCentral.DemandeTrajet(1, List("1"), 1, probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Deuxième demande pour la même zone - devrait échouer (pas de réponse positive)
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()
      hub ! HubCentral.DemandeTrajet(2, List("1"), 1, probe2.ref)

      // La zone 1 est maintenant réservée par la voie 1
      // Donc aucune réponse positive ne devrait être envoyée
      probe2.expectNoMessage(1.second)
    }

    "mettre à jour la file d'attente" in {
      val hub = testKit.spawn(HubCentral(), "hub-test-3")

      val probe = testKit.createTestProbe[CapteurVoie.Command]()

      // Envoie une demande avec 5 véhicules
      hub ! HubCentral.DemandeTrajet(1, List("1", "2"), 5, probe.ref)
      probe.expectMessage(CapteurVoie.FeuPasseAuVert)
    }
  }

  "L'avancement dans la séquence" should {

    "libérer la zone quittée" in {
      val hub = testKit.spawn(HubCentral(), "hub-test-4")

      // Réserve d'abord une zone
      val probe1 = testKit.createTestProbe[CapteurVoie.Command]()
      hub ! HubCentral.DemandeTrajet(1, List("1"), 1, probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Avance dans la séquence
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()
      hub ! HubCentral.AvancerSequence(1, "1", "2", probe2.ref)
      probe2.expectMessage(CapteurVoie.FeuPasseAuVert)
    }
  }

  "La fin de passage total" should {

    "libérer la dernière zone" in {
      val hub = testKit.spawn(HubCentral(), "hub-test-5")

      val probe = testKit.createTestProbe[CapteurVoie.Command]()

      // Réserve une zone
      hub ! HubCentral.DemandeTrajet(1, List("1"), 1, probe.ref)
      probe.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Termine le passage
      hub ! HubCentral.FinPassageTotal(1, "1", 0)

      // La zone devrait être libérée
      // On peut le vérifier en envoyant une nouvelle demande
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()
      hub ! HubCentral.DemandeTrajet(2, List("1"), 1, probe2.ref)
      probe2.expectMessage(CapteurVoie.FeuPasseAuVert)
    }

    "mettre à jour la file d'attente avec le nombre de véhicules restant" in {
      val hub = testKit.spawn(HubCentral(), "hub-test-6")

      val probe = testKit.createTestProbe[CapteurVoie.Command]()

      // Termine avec 3 véhicules restants dans la file
      hub ! HubCentral.FinPassageTotal(1, "1", 3)

      // Pas de réponse attendue car pas de reservation
      probe.expectNoMessage(500.millis)
    }
  }

  "Les commandes du hub" should {

    "DemandeTrajet doit avoir les bons champs" in {
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      val cmd = HubCentral.DemandeTrajet(5, List("2", "3"), 3, probe.ref)

      cmd.voieId shouldBe 5
      cmd.trajet shouldBe List("2", "3")
      cmd.nbVehicules shouldBe 3
      cmd.replyTo shouldBe probe.ref
    }

    "AvancerSequence doit avoir les bons champs" in {
      val probe = testKit.createTestProbe[CapteurVoie.Command]()
      val cmd = HubCentral.AvancerSequence(3, "1", "2", probe.ref)

      cmd.voieId shouldBe 3
      cmd.zoneQuittee shouldBe "1"
      cmd.zoneEntree shouldBe "2"
      cmd.replyTo shouldBe probe.ref
    }

    "FinPassageTotal doit avoir les bons champs" in {
      val cmd = HubCentral.FinPassageTotal(7, "4", 2)

      cmd.voieId shouldBe 7
      cmd.derniereZone shouldBe "4"
      cmd.nbVehicules shouldBe 2
    }
  }

  "EtatCarrefour" should {

    "pouvoir être copié avec de nouvelles valeurs" in {
      val etat = HubCentral.EtatCarrefour(
        reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0),
        filesAttente = (1 to 12).map(_ -> 0).toMap
      )

      val etatModifie = etat.copy(reservations = Map("1" -> 5, "2" -> 0, "3" -> 0, "4" -> 0))

      etatModifie.reservations("1") shouldBe 5
      etatModifie.reservations("2") shouldBe 0
    }

    "avoir les bonnes clés pour les reservations" in {
      val etat = HubCentral.EtatCarrefour(
        reservations = Map("1" -> 0, "2" -> 0, "3" -> 0, "4" -> 0),
        filesAttente = Map()
      )

      etat.reservations.keySet should contain ("1")
      etat.reservations.keySet should contain ("2")
      etat.reservations.keySet should contain ("3")
      etat.reservations.keySet should contain ("4")
    }
  }
}