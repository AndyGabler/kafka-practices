package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
public class GameResultToDebeziumSinkMapper implements ValueMapper<JsonNode, JsonNode> {
    @Override
    public JsonNode apply(JsonNode gameResult) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode debeziumMessage = mapper.createObjectNode();

        final ObjectNode schema = debeziumMessage.objectNode();
        schema.put("type", "struct");
        schema.put("optional", false);
        schema.put("name", "game_result");

        final ArrayNode fieldsArrayNode = schema.arrayNode();

        final ObjectNode idFieldDefinitionNode = fieldsArrayNode.objectNode();
        idFieldDefinitionNode.put("field", "id");
        idFieldDefinitionNode.put("type", "int32");
        idFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(idFieldDefinitionNode);

        final ObjectNode matchDateFieldDefinitionNode = fieldsArrayNode.objectNode();
        matchDateFieldDefinitionNode.put("field", "match_date");
        matchDateFieldDefinitionNode.put("type", "int32");
        matchDateFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(matchDateFieldDefinitionNode);

        final ObjectNode homeTeamFieldDefinitionNode = fieldsArrayNode.objectNode();
        homeTeamFieldDefinitionNode.put("field", "home_team");
        homeTeamFieldDefinitionNode.put("type", "string");
        homeTeamFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(homeTeamFieldDefinitionNode);

        final ObjectNode visitingTeamFieldDefinitionNode = fieldsArrayNode.objectNode();
        visitingTeamFieldDefinitionNode.put("field", "visiting_team");
        visitingTeamFieldDefinitionNode.put("type", "string");
        visitingTeamFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(visitingTeamFieldDefinitionNode);

        final ObjectNode homeTeamScoreFieldDefinitionNode = fieldsArrayNode.objectNode();
        homeTeamScoreFieldDefinitionNode.put("field", "home_team_score");
        homeTeamScoreFieldDefinitionNode.put("type", "int32");
        homeTeamScoreFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(homeTeamScoreFieldDefinitionNode);

        final ObjectNode visitingTeamScoreFieldDefinitionNode = fieldsArrayNode.objectNode();
        visitingTeamScoreFieldDefinitionNode.put("field", "visiting_team_score");
        visitingTeamScoreFieldDefinitionNode.put("type", "int32");
        visitingTeamScoreFieldDefinitionNode.put("optional", false);
        fieldsArrayNode.add(visitingTeamScoreFieldDefinitionNode);

        schema.set("fields", fieldsArrayNode);
        debeziumMessage.set("schema", schema);

        debeziumMessage.set("payload", gameResult);

        return debeziumMessage;
    }
}
