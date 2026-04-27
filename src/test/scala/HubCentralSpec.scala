import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

class HubCentralSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }

  "HubCentral" should {

    "s'initialiser correctement" in {
      val hub = testKit.spawn(HubCentral(), "hub-init")
      hub should not be null
    }

    "accorder le feu vert si les zones demandées sont libres" in {
      val hub = testKit.spawn(HubCentral(), "hub-libre")
      val probe = testKit.createTestProbe[CapteurVoie.Command]()

      // Voie 1 demande les zones 1 et 2
      hub ! HubCentral.DemandeTrajet(1, List("1", "2"), 1, probe.ref)
      
      // Les zones sont libres, le feu passe au vert
      probe.expectMessage(CapteurVoie.FeuPasseAuVert)
    }

    "refuser le feu vert (ignorer la demande) si une zone est déjà réservée" in {
      val hub = testKit.spawn(HubCentral(), "hub-occupe")
      val probe1 = testKit.createTestProbe[CapteurVoie.Command]()
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()

      // Voie 1 réserve la zone 1 (succès)
      hub ! HubCentral.DemandeTrajet(1, List("1"), 1, probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Voie 2 tente de réserver la zone 1 en même temps
      hub ! HubCentral.DemandeTrajet(2, List("1"), 1, probe2.ref)
      
      // Le hub doit ignorer la demande de la Voie 2 (pas de réponse) pour éviter un crash
      probe2.expectNoMessage(1.second)
    }

    "libérer la zone quittée lors de l'avancement, permettant à un autre d'y entrer" in {
      val hub = testKit.spawn(HubCentral(), "hub-avancement")
      val probe1 = testKit.createTestProbe[CapteurVoie.Command]()
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()

      // Voie 1 réserve les zones 1 et 2
      hub ! HubCentral.DemandeTrajet(1, List("1", "2"), 1, probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Voie 1 signale qu'elle quitte la zone 1 pour entrer dans la 2
      hub ! HubCentral.AvancerSequence(1, "1", "2", probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // POUR PROUVER que la zone 1 est libérée, la Voie 2 la demande :
      hub ! HubCentral.DemandeTrajet(2, List("1"), 1, probe2.ref)
      
      // Succès ! La zone 1 était bien redevenue libre.
      probe2.expectMessage(CapteurVoie.FeuPasseAuVert)
    }

    "libérer complètement les zones à la fin du passage total" in {
      val hub = testKit.spawn(HubCentral(), "hub-fin")
      val probe1 = testKit.createTestProbe[CapteurVoie.Command]()
      val probe2 = testKit.createTestProbe[CapteurVoie.Command]()

      // Voie 1 réserve la zone 3
      hub ! HubCentral.DemandeTrajet(1, List("3"), 1, probe1.ref)
      probe1.expectMessage(CapteurVoie.FeuPasseAuVert)

      // Voie 1 termine totalement son passage et quitte le carrefour
      hub ! HubCentral.FinPassageTotal(1, "3", 0)

      // La Voie 2 demande la zone 3
      hub ! HubCentral.DemandeTrajet(2, List("3"), 1, probe2.ref)
      
      // Succès ! La zone 3 a bien été nettoyée par la FinPassageTotal
      probe2.expectMessage(CapteurVoie.FeuPasseAuVert)
    }
  }
}