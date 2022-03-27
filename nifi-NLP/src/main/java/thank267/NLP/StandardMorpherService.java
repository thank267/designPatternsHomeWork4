/*

 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package thank267.NLP;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.conversions.Bson;
import ru.morpher.ws3.AccessDeniedException;
import ru.morpher.ws3.ArgumentEmptyException;
import ru.morpher.ws3.Client;
import ru.morpher.ws3.ClientBuilder;
import ru.morpher.ws3.russian.ArgumentNotRussianException;
import ru.morpher.ws3.russian.DeclensionResult;
import ru.morpher.ws3.russian.InvalidFlagsException;
import ru.morpher.ws3.russian.NumeralsDeclensionNotSupportedException;
import thank267.commons.models.*;

import java.io.IOException;
import java.util.*;

@Tags({"NLP lemma corpus case morpher"})
@CapabilityDescription("NLP lemma corpus case morpher")
public class StandardMorpherService extends AbstractControllerService implements MorpherService {

	public static final PropertyDescriptor TOKEN = new PropertyDescriptor.Builder().name("TOKEN").displayName("TOKEN for morpher.ru service").description("TOKEN").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor MONGODB_URI = new PropertyDescriptor.Builder().name("MONGODB_URI").displayName("MongoDB Uri").description("MongoDB Uri").required(true).addValidator(StandardValidators.URI_VALIDATOR).build();
	protected MongoClient mongoClient;
	protected MongoDatabase mongoDB;
	protected MongoCollection collection;
	private String token;

	public StandardMorpherService() {
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

		List<PropertyDescriptor> _temp = new ArrayList<>();

		_temp.add(TOKEN);
		_temp.add(MONGODB_URI);

		return Collections.unmodifiableList(_temp);
	}

	@OnEnabled
	public void onEnabled(final ConfigurationContext context) {
		this.token = context.getProperty(TOKEN).getValue();

		try {
			String uri = context.getProperty(MONGODB_URI).getValue();

			mongoClient = MongoClients.create(uri);

		} catch (Exception e) {
			getLogger().error("Failed to schedule {} due to {}", new Object[]{getClass().getName(), e}, e);

			throw e;
		}

		super.enabled();
	}

	public Document execute(final String source, final String cs, final String plural, final String pos, final String gender, final String person) throws ProcessException {

		getLogger().info("Начинаем выполнение {}", source);

		Document check = null;

		CaseEnum ce = null;

		NumberEnum ne = null;

		PosEnum pe = null;

		GenderEnum ge = null;

		PersonEnum pere = null;

		try {

			if (StringUtils.isNotBlank(cs)) {
				ce = CaseEnum.get(cs);
			}

			if (StringUtils.isNotBlank(plural)) {
				ne = NumberEnum.valueOf(plural);
			}

			if (StringUtils.isNotBlank(pos)) {
				pe = PosEnum.valueOf(pos);
			}

			if (StringUtils.isNotBlank(gender)) {
				ge = GenderEnum.valueOf(gender);
			}

			if (StringUtils.isNotBlank(person)) {
				pere = PersonEnum.get(person);
			}

			// смотрим Золотую лемму в mongo
			Document tmpName = stage1(source, ce, ne, pe, ge, pere);

			// смотрим лемму в кэше
			if (!Optional.ofNullable(tmpName).isPresent()) {
				tmpName = stage2(source, ce, ne, pe, ge, pere);
			}

			// если нет ни в mongo ни в кэше идем на morpher.ru и записываем в кэш
			if (!Optional.ofNullable(tmpName).isPresent()) {
				tmpName = stage3(source, ce, ne, pe, ge, pere);
			}

			check = Optional.ofNullable(tmpName).orElseThrow(() -> {
				throw new ProcessException(String.format("Не нашли лемму нигде %s %s", source, cs));
			});

		} catch (Exception e) {
			throw new ProcessException(e);
		}

		return check;

	}

	@OnDisabled
	public void closeClient() {

		if (mongoClient != null) {
			getLogger().info("Closing MongoClient");
			mongoClient.close();
			mongoClient = null;
		}

	}

	protected MongoDatabase getDatabase() {
		return mongoClient.getDatabase("nlp");
	}

	protected MongoCollection<Document> getCollection(String type) {
		return getDatabase().getCollection(type);
	}

	public Document stage1(final String source, final CaseEnum cs, final NumberEnum plural, final PosEnum pos, final GenderEnum gennder, final PersonEnum person) {

		collection = getCollection("lemmas");

		List<Bson> filters = new ArrayList<Bson>();

		filters.add(Filters.eq("lemma", source.toLowerCase()));

		Optional.ofNullable(cs).ifPresent(cS -> {
					filters.add(Filters.eq("source.case", cS.name()));
				}
		);

		Optional.ofNullable(pos).ifPresent(p -> {

					filters.add(Filters.eq("pos", p.name()));

					if (p == PosEnum.VERB) {

						Optional.ofNullable(gennder).ifPresent(gn -> {

							filters.add(Filters.eq("source.gender", gn.name()));
						});

						Optional.ofNullable(person).ifPresent(pn -> {

							filters.add(Filters.eq("source.person", pn.face()));
						});

					}
				}

		);

		Optional.ofNullable(plural).ifPresent(pl -> {

					filters.add(Filters.eq("source.number", pl.name()));

				}

		);

		filters.forEach(filter -> getLogger().info(String.format("Смотрим filter 1 ! : %s", filter)));

		List<Bson> finders = Arrays.asList(Aggregates.unwind("$source"), Aggregates.match(Filters.and(filters)));

	    return (Document) collection.aggregate(finders

		).first();

	}

	public Document stage2(final String source, final CaseEnum cs, final NumberEnum plural, final PosEnum pos, final GenderEnum gennder, final PersonEnum person) {

		collection = getCollection("cache");

		List<Bson> filters = new ArrayList<Bson>();

		filters.add(Filters.eq("_id", source.toLowerCase().replaceAll("\\s+", "_")));

		Optional.ofNullable(cs).ifPresent(cS -> {

					filters.add(Filters.eq("source.case", cS.name()));

				}

		);

		Optional.ofNullable(pos).ifPresent(p -> {

					filters.add(Filters.eq("pos", p.name()));

					if (p == PosEnum.VERB) {

						Optional.ofNullable(gennder).ifPresent(gn -> {

							filters.add(Filters.eq("source.gender", gn.name()));
						});

						Optional.ofNullable(person).ifPresent(pn -> {

							filters.add(Filters.eq("source.person", pn.face()));
						});

					}
				}

		);

		Optional.ofNullable(plural).ifPresent(pl -> {

					filters.add(Filters.eq("source.number", pl.name()));

				}

		);

		List<Bson> finders = Arrays.asList(Aggregates.unwind("$source"), Aggregates.match(Filters.and(filters)));

		return (Document) collection.aggregate(finders

		).first();

	}

	public Document stage3(final String source, final CaseEnum cs, final NumberEnum plural, final PosEnum pos, final GenderEnum gennder, final PersonEnum person) throws IOException, NumeralsDeclensionNotSupportedException, ArgumentNotRussianException, InvalidFlagsException, ArgumentEmptyException, AccessDeniedException {

		if (cs == null) {
			return stage2(source, cs, plural, pos, gennder, person);
		}

		Map<CaseEnum, String> map = new HashMap<>();

		map.put(CaseEnum.Nom, "nominative");
		map.put(CaseEnum.Gen, "genitive");
		map.put(CaseEnum.Dat, "dative");
		map.put(CaseEnum.Acc, "accusative");
		map.put(CaseEnum.Ins, "instrumental");
		map.put(CaseEnum.Loc, "prepositional");

		Client client = new ClientBuilder().useToken(token).build();

		DeclensionResult russianDeclensionResult = client.russian().declension(source);

		Document document = new Document().append("_id", source.toLowerCase().replaceAll("\\s+", "_")).append("retension", new Date());

		List<Document> cases = new ArrayList<>();

		// добавляем единственное число

		map.entrySet().stream().forEach(e -> {

			try {

				if (StringUtils.isNotBlank((String) russianDeclensionResult.getClass().getField(e.getValue()).get(russianDeclensionResult))) {

					Document doc = new Document("source", russianDeclensionResult.getClass().getField(e.getValue()).get(russianDeclensionResult)).append("case", e.getKey().name()).append("number", NumberEnum.Sing.name());

					cases.add(doc);

				} else {
					throw new ProcessException(String.format("Не получили %s падеж %s от morpher.ru", e.getKey(), source));
				}

			} catch (NoSuchFieldException err) {

				throw new ProcessException(err);
			} catch (IllegalArgumentException err) {
				// TODO Auto-generated catch block
				throw new ProcessException(err);
			} catch (IllegalAccessException err) {
				// TODO Auto-generated catch block
				throw new ProcessException(err);
			} catch (SecurityException err) {
				// TODO Auto-generated catch block
				throw new ProcessException(err);
			}

		});

		// добавляем множественное число

		if (russianDeclensionResult.plural != null) {

			map.entrySet().stream().forEach(e -> {

				try {

					if (StringUtils.isNotBlank((String) russianDeclensionResult.plural.getClass().getField(e.getValue()).get(russianDeclensionResult.plural))) {

						Document doc = new Document("source", russianDeclensionResult.plural.getClass().getField(e.getValue()).get(russianDeclensionResult.plural)).append("case", e.getKey().name()).append("number", NumberEnum.Plur.name());

						cases.add(doc);

					} else {
						throw new ProcessException(String.format("Не получили %s падеж %s от morpher.ru", e.getKey(), source));
					}

				} catch (NoSuchFieldException err) {

					throw new ProcessException(err);
				} catch (IllegalArgumentException err) {
					// TODO Auto-generated catch block
					throw new ProcessException(err);
				} catch (IllegalAccessException err) {
					// TODO Auto-generated catch block
					throw new ProcessException(err);
				} catch (SecurityException err) {
					// TODO Auto-generated catch block
					throw new ProcessException(err);
				}

			});

		} else {
			getLogger().info(String.format("Для source %s нет множественного числа", source));
		}

		document.append("source", cases);

		collection = getCollection("cache");

		collection.insertOne(document);

		return stage2(source, cs, plural, pos, gennder, person);

	}

}
