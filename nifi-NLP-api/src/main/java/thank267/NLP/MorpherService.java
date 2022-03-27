package thank267.NLP;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.bson.Document;

import thank267.commons.models.SpellOut;

@Tags({"NLP lemma corpus case morpher"})
@CapabilityDescription("NLP lemma corpus case morpher")
public abstract interface MorpherService
  extends ControllerService
  {
	  public abstract Document execute(final String source, final String cs, final String plural, final String  pos, final String gennder, final String person)
	    throws ProcessException;
  }
