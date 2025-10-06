package vivek.example.kite.common.service

import org.springframework.stereotype.Service
import vivek.example.kite.common.config.CommonProperties

/**
 * A service responsible for providing the master list of all stock symbols.
 *
 * In a production system, this would fetch symbols from a database, a remote configuration service,
 * or a daily file from the exchange. For our application, it serves as an abstraction over the list
 * defined in our application-local.yml, making it easy to swap out the source later.
 */
@Service
class SymbolService(private val commonProperties: CommonProperties) {

  /** Returns a distinct list of all known stock symbols. */
  fun getAllSymbols(): List<String> {
    // In this implementation, we derive the master list from the mock producer's configuration.
    return commonProperties.stocks.distinct()
  }
}
