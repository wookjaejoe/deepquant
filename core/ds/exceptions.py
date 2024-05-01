class AuthenticationError(Exception):
    def __str__(self):
        return "Authentication error occurred. Check your token."

