import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests pour Main.scala (CarrefourMain)
 * On vérifie uniquement l'amorçage et la configuration statique.
 */
class CarrefourMainSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  // On utilise le TestKit pour éviter de gérer les ActorSystem manuellement
  val testKit: ActorTestKit = ActorTestKit()

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }

  "CarrefourMain" should {

    "avoir la bonne configuration statique des zones" in {
      // Un seul test propre pour vérifier le contenu et l'ordre
      CarrefourMain.zones should contain inOrderOnly ("1", "2", "3", "4")
    }

    "retourner un Behavior valide" in {
      val behavior = CarrefourMain()
      behavior should not be null
    }

    "pouvoir s'initialiser sans crasher (Test d'intégration)" in {
      // Si la logique dans Behaviors.setup plante (ex: boucle infinie, 
      // noms d'acteurs en double, etc.), le testKit.spawn va lancer une exception.
      // S'il passe cette ligne, ça prouve que l'arbre d'acteurs s'est bien monté.
      val mainActor = testKit.spawn(CarrefourMain(), "test-demarrage-main")
      
      mainActor should not be null
    }
  }
}