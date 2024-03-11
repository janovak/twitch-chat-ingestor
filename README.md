# Twitch Chat Ingestor

While Twitch provides methods to listen to messages that are currently happening, it doesn't provide a way to query old messages. This project's REST API and the microservices that back it provide  functionality to users who want to query old messages for a given streamer.

### REST API

The `broadcaster_id` API provides a way for callers to query the Cassandra database that houses all the chat messages.

- **Endpoint**: `/v1.0/<int:broadcaster_id>/chat`
- **Parameters**:
  - `start` [required]: ISO 8601 timestamp denoting the beginning of the time range you would like to search
  - `end` [required]: ISO 8601 timestamp denoting the end of the time range you would like to search
  - `after` [optional]: Used for pagination. Users can pass the cursor they received from previous calls when the data returned as part of the previous call was only a portion of the data described by the query.
  - `limit` [optional]: A number between 1 and 100, inclusive, that defines the maximum number of messages to be returned by the API. If no limit parameter is specified, the API will return a maximum of 20 messages.

**Sample Request:**
```bash
curl -X GET "http://localhost:5000/v1.0/24538518/chat?start=2024-01-10T01:01:01.011Z&end=2024-01-30T23:59:59.000Z&limit=2&after=3Cj7V5upmmx1b6rawVelGDvOKUpMIqwCaC9cW2zqqDQbzLM7d22rBjRAnMUNT04afSQDqRWpEwSO2DPNSXkJmCbEg"
```

***Sample Response:**
```
{
  "cursor": "3Cj7V5upmmx1b6rawVelGDvOKUpMIqwCaDZQMbi08XVq9jnNijXGgqS7nEEvHjW8SHXOSIvoYySPDIN6NbXQ52l79",
  "messages": [
    {
      "broadcaster_id": 24538518,
      "message": "{\"text\": \"@uhblivean24 ofc in the game eavShy\", \"is_me\": false, \"bits\": 0, \"sent_timestamp\": 1706408359963, \"reply_parent_msg_id\": null, \"reply_parent_user_id\": null, \"reply_parent_user_login\": null, \"reply_parent_display_name\": null, \"reply_parent_msg_body\": null, \"reply_thread_parent_msg_id\": null, \"reply_thread_parent_user_login\": null, \"emotes\": {\"emotesv2_a439839fd0a34aceb1712e348bf22cb8\": [{\"start_position\": \"29\", \"end_position\": \"34\"}]}, \"id\": \"0a232bf9-2e6a-4bd4-85e3-c9800b68fedb\", \"room\": {\"name\": \"sneakylol\", \"is_emote_only\": false, \"is_subs_only\": false, \"is_followers_only\": true, \"is_unique_only\": false, \"follower_only_delay\": 0, \"room_id\": \"24538518\", \"slow\": 0}, \"user\": {\"name\": \"cutie_amelka\", \"badge_info\": null, \"badges\": {\"premium\": \"1\"}, \"color\": \"#8A2BE2\", \"display_name\": \"Cutie_amelka\", \"mod\": false, \"subscriber\": false, \"turbo\": false, \"id\": \"212750661\", \"user_type\": null, \"vip\": false}}",
      "message_id": "0a232bf9-2e6a-4bd4-85e3-c9800b68fedb",
      "timestamp": 1706408359963
    },
    {
      "broadcaster_id": 24538518,
      "message": "{\"text\": \"@Uhblivean24 trying my best Suffering\", \"is_me\": false, \"bits\": 0, \"sent_timestamp\": 1706408361811, \"reply_parent_msg_id\": \"178293f6-b521-4d25-b808-9d36cc373069\", \"reply_parent_user_id\": \"39988164\", \"reply_parent_user_login\": \"uhblivean24\", \"reply_parent_display_name\": \"Uhblivean24\", \"reply_parent_msg_body\": \"@cakebud\\\\sironmouseACTUALLY\\\\sfigure\\\\sit\\\\sout\", \"reply_thread_parent_msg_id\": \"178293f6-b521-4d25-b808-9d36cc373069\", \"reply_thread_parent_user_login\": \"uhblivean24\", \"emotes\": null, \"id\": \"435c1723-5d6f-4aee-927d-6f9f50889e0f\", \"room\": {\"name\": \"sneakylol\", \"is_emote_only\": false, \"is_subs_only\": false, \"is_followers_only\": true, \"is_unique_only\": false, \"follower_only_delay\": 0, \"room_id\": \"24538518\", \"slow\": 0}, \"user\": {\"name\": \"cakebud\", \"badge_info\": {\"subscriber\": \"38\"}, \"badges\": {\"vip\": \"1\", \"subscriber\": \"36\"}, \"color\": \"#FF69B4\", \"display_name\": \"CakeBud\", \"mod\": false, \"subscriber\": true, \"turbo\": false, \"id\": \"94880631\", \"user_type\": null, \"vip\": true}}",
      "message_id": "435c1723-5d6f-4aee-927d-6f9f50889e0f",
      "timestamp": 1706408361811
    }
  ]
}
```

### Streamer Listener

The streamer listener service regularly polls Twitch's `streams` API to get the streamers that are currently live. It then publishes that list of streamers to a message queue for any consumer that needs to operate on the currently live streamers.

### Streamer Ingestion

The streamer ingestion service listens to the aforementioned message queue and updates a streamer database with any streamers that are not yet in the database.

### Chat Listener

The chat listener service also listens to the aforementioned message queue for live streamers. It uses the data from the message queue to keep a cache of currently live streamers. While streamers are online, it publishes all messages sent in the streamer's chat to another message queue for processing.

### Chat Ingestor

The chat ingestion service listens to the chat message queue and writes all messages to a Cassandra database.
