import React from "react";
import { Amplify } from "aws-amplify";
import { Authenticator } from "@aws-amplify/ui-react";
import "@aws-amplify/ui-react/styles.css";
import awsExports from "../../aws-exports";
import "bootstrap/dist/css/bootstrap.min.css";
import PurchaseTP from "./PurchaseTP";

Amplify.configure(awsExports);

const Account = () => {
  return (
    <div className="account-page">
      <div className="container-fluid">
        <div className="auth-container">
          <Authenticator>
            {({ signOut, user }) => (
              <main>
                <header className="account-header text-center my-4">
                  <h1>Welcome {user?.signInDetails?.loginId}</h1>
                </header>
                <PurchaseTP />
                <div className="text-center my-4">
                  <button className="btn btn-primary" onClick={signOut}>
                    Sign Out
                  </button>
                </div>
              </main>
            )}
          </Authenticator>
        </div>
      </div>
    </div>
  );
};

export default Account;
