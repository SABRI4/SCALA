import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

class CapteurVoieSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(15.seconds, 100.millis)

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }

  "CapteurVoie" should {

    "retourner un Behavior valide à l'initialisation" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val behavior = CapteurVoie(1, "1", hub.ref)
      behavior should not be null
    }

    "envoyer une DemandeTrajet avec le bon ID après l'arrivée d'un véhicule" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val voieId = 7
      val capteur = testKit.spawn(CapteurVoie(voieId, "1", hub.ref))

      // On force l'arrivée pour garantir l'envoi d'un message sans dépendre du hasard
      capteur ! CapteurVoie.ArriveeVehicule
      
      val msg = hub.expectMessageType[HubCentral.DemandeTrajet](3.seconds)
      msg.voieId shouldBe voieId
    }

    "réagir au FeuPasseAuVert en traitant le véhicule (AvancerSequence ou FinPassage)" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      val capteur = testKit.spawn(CapteurVoie(1, "1", hub.ref))
      
      // 1. On force l'arrivée d'un véhicule pour être sûr qu'il y en a au moins un
      capteur ! CapteurVoie.ArriveeVehicule
      
      // 2. On donne le feu vert (le timer de 1.5s se lance dans l'acteur)
      capteur ! CapteurVoie.FeuPasseAuVert

      // 3. Boucle de filtrage intelligente
      // On lit les messages un par un. On ignore les "DemandeTrajet" jusqu'à
      // recevoir la vraie réponse (AvancerSequence ou FinPassageTotal).
      var reponseTrouvee = false
      
      while (!reponseTrouvee) {
        // S'il n'y a plus de messages au bout de 4 secondes, ça plantera automatiquement (Timeout)
        val msg = hub.receiveMessage(4.seconds) 
        
        msg match {
          case _: HubCentral.AvancerSequence => 
            reponseTrouvee = true
            succeed // Le test est un succès !
            
          case _: HubCentral.FinPassageTotal => 
            reponseTrouvee = true
            succeed // Le test est un succès !
            
          case _: HubCentral.DemandeTrajet => 
            // C'est un message d'initialisation/affichage, on l'ignore et la boucle continue
        }
      }
    }

    "envoyer ArriveeVehicule périodiquement via son générateur de flux" in {
      val hub = testKit.createTestProbe[HubCentral.HubCommand]()
      
      // On utilise le VRAI capteur qui possède le vrai timer interne
      val capteur = testKit.spawn(CapteurVoie(99, "1", hub.ref))

      // Le timer met entre 5 et 11 secondes. 
      // On attend deux messages de DemandeTrajet espacés dans le temps pour prouver que le flux est continu.
      hub.expectMessageType[HubCentral.DemandeTrajet](15.seconds)
      hub.expectMessageType[HubCentral.DemandeTrajet](15.seconds)
    }

    "respecter les transitions pour toutes les zones possibles" in {
      val transitionsAttendues = Map(
        "1" -> "3",
        "2" -> "1",
        "3" -> "4",
        "4" -> "2"
      )

      for ((zoneInitiale, zoneSuivanteAttendue) <- transitionsAttendues) {
        // CORRECTION CRUCIALE : Le TestProbe DOIT être recréé à chaque itération 
        // sinon il lit les messages des zones testées précédemment !
        val hubLocal = testKit.createTestProbe[HubCentral.HubCommand]()
        val capteur = testKit.spawn(CapteurVoie(1, zoneInitiale, hubLocal.ref))
        
        capteur ! CapteurVoie.ArriveeVehicule
        
        val msg = hubLocal.expectMessageType[HubCentral.DemandeTrajet](3.seconds)
        val trajet = msg.trajet

        // Le premier élément doit toujours être la zone cible
        trajet.head shouldBe zoneInitiale
        
        // Si le trajet a 2 zones, on vérifie que la transition est valide
        if (trajet.size == 2) {
          trajet(1) shouldBe zoneSuivanteAttendue
        }
      }
    }
  }
}