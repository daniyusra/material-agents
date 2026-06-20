import { createContext, useContext, useEffect, useState, ReactNode } from "react";
import { blogApi } from "../api/blogApi";

interface AuthContextValue {
  isAdmin: boolean;
  isLoading: boolean;
  login(username: string, password: string): Promise<void>;
  logout(): Promise<void>;
}

const AuthContext = createContext<AuthContextValue>({
  isAdmin: false,
  isLoading: true,
  login: async () => {},
  logout: async () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAdmin, setIsAdmin] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    blogApi
      .me()
      .then(({ authenticated }) => setIsAdmin(authenticated))
      .catch(() => setIsAdmin(false))
      .finally(() => setIsLoading(false));
  }, []);

  async function login(username: string, password: string) {
    await blogApi.login(username, password);
    setIsAdmin(true);
  }

  async function logout() {
    await blogApi.logout().catch(() => {});
    setIsAdmin(false);
  }

  return (
    <AuthContext.Provider value={{ isAdmin, isLoading, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
