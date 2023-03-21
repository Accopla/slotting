import pickle
from pathlib import Path
import streamlit as st 
import streamlit_authenticator as stauth
import yaml
import bcrypt
import jwt



#names = ["Julio Cesar", "Abel Rocha", "Santa Cruz"]
#usernames = ["JC", "AR", "ST"]
passwords = ["123", "456"]

hashed_passwords = stauth.Hasher(passwords).generate()
print(hashed_passwords)

#file_path = Path(__file__).parent / "hashed_pw.pkl"
#with file_path.open("wb") as file:
    #pickle.dump(hashed_passwords, file)



