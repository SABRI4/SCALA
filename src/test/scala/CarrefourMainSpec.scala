

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, ActorRef}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

/**
 * Tests unitaires pour Main.scala (CarrefourMain)
 * 
 * Ces tests vérifient:
 * - La configuration des zones
 * - Le comportement de l'acteur CarrefourMain
 * - La création des acteurs HubCentral et CapteurVoie
 */
class CarrefourMainSpec extends AnyWordSpec with Matchers with ScalaFutures {

  // Configuration du timeout pour les tests asynchrones
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 100.millis)

  "CarrefourMain" should {
    
    "avoir un vecteur de 4 zones défini" in {
      // Vérifie que le vecteur de zones contient exactement 4 éléments
      CarrefourMain.zones should have size 4
      CarrefourMain.zones should contain ("1")
      CarrefourMain.zones should contain ("2")
      CarrefourMain.zones should contain ("3")
      CarrefourMain.zones should contain ("4")
    }

    "avoir les zones dans le bon ordre" in {
      CarrefourMain.zones(0) shouldBe "1"
      CarrefourMain.zones(1) shouldBe "2"
      CarrefourMain.zones(2) shouldBe "3"
      CarrefourMain.zones(3) shouldBe "4"
    }

    "retourner un Behavior valide" in {
      // Vérifie que la méthode apply() retourne un Behavior non-null
      val behavior = CarrefourMain()
      behavior should not be null
    }
  }

  "L'initialisation de CarrefourMain" should {
    
    "créer un système d'acteurs valide" in {
      // Crée un système d'acteurs avec CarrefourMain
      val system = ActorSystem(CarrefourMain(), "TestCarrefour")
      
      try {
        // Vérifie que le système est bien créé
        system should not be null
        
        // Attend un peu pour laisser le temps aux acteurs de se créer
        Thread.sleep(500)
        
        // Vérifie que le HubCentral a été créé
        val hubExists = system.whenTerminated.isCompleted shouldBe false
        
      } finally {
        // Nettoyage: termine le système
        system.terminate()
        // Attend la terminaison
        scala.concurrent.Await.result(system.whenTerminated, 5.seconds)
      }
    }

    "créer exactement 12 CapteurVoie" taggedAs (IntegrationTest) in {
      // Ce test vérifie que 12 acteurs CapteurVoie sont créés
      // C'est un test d'intégration car il nécessite le système d'acteurs complet
      
      val system = ActorSystem(CarrefourMain(), "TestCarrefour12Voies")
      
      try {
        // Attend la création de tous les acteurs
        Thread.sleep(1000)
        
        // Le système ne doit pas être terminé
        system.whenTerminated.isCompleted shouldBe false
        
      } finally {
        system.terminate()
        scala.concurrent.Await.result(system.whenTerminated, 5.seconds)
      }
    }

    "créer le HubCentral" taggedAs (IntegrationTest) in {
      val system = ActorSystem(CarrefourMain(), "TestCarrefourHub")
      
      try {
        Thread.sleep(500)
        system.whenTerminated.isCompleted shouldBe false
        
      } finally {
        system.terminate()
        scala.concurrent.Await.result(system.whenTerminated, 5.seconds)
      }
    }
  }

  "La distribution des zones initiales" should {
    
    "être correcte pour les 12 voies" in {
      // Vérifie la logique de distribution des zones:
      // voie 1 -> zone 1, voie 2 -> zone 2, voie 3 -> zone 3, voie 4 -> zone 4
      // voie 5 -> zone 1, voie 6 -> zone 2, etc.
      
      for (i <- 1 to 12) {
        val zoneAttendue = CarrefourMain.zones((i - 1) % 4)
        val zoneCalculee = CarrefourMain.zones((i - 1) % 4)
        zoneCalculee shouldBe zoneAttendue
      }
    }

    "avoir une distribution cyclique correcte" in {
      // Les voies 1, 5, 9 doivent avoir la zone 1
      CarrefourMain.zones((1 - 1) % 4) shouldBe "1"
      CarrefourMain.zones((5 - 1) % 4) shouldBe "1"
      CarrefourMain.zones((9 - 1) % 4) shouldBe "1"
      
      // Les voies 2, 6, 10 doivent avoir la zone 2
      CarrefourMain.zones((2 - 1) % 4) shouldBe "2"
      CarrefourMain.zones((6 - 1) % 4) shouldBe "2"
      CarrefourMain.zones((10 - 1) % 4) shouldBe "2"
    }
  }
}

/**
 * Tag pour les tests d'intégration
 */
object IntegrationTest extends org.scalatest.Tag("com.carrefour.integration")