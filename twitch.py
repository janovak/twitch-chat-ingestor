import auth.secrets as secrets
from twitchAPI.twitch import Twitch
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.type import AuthScope

async def get_session():
    USER_SCOPE = [AuthScope.CHAT_READ]

    twitch = await Twitch(secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret())
    auth = UserAuthenticator(twitch, USER_SCOPE)
    token, refresh_token = await auth.authenticate()
    await twitch.set_user_authentication(token, USER_SCOPE, refresh_token)

    return twitch