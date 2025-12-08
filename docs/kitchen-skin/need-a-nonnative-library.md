# Need A NonNative Library?

If the user wants to use a non-native library (ex: a js sdk with no python equivalent), the user can create a resource where they spin up a "side car process" and create adapater methods on the class that call the appropriate methods of the non-native library and handle the IPC / type conversions. This approach let's the user meet thier requirements in a way that is safe to the minions runtime since exceptions in the non native lib won't crash the runtime because the non native lib is in a "side car process".
