import { Navigate, useLocation } from "react-router-dom";
import { useAuth } from "../../contexts/AuthContext";
import { ReactNode } from "react";

interface Props {
  children: ReactNode;
}

/**
 * Frontend route guard.
 *
 * NOTE: This is a UX convenience only — it is NOT a security boundary.
 * Unauthenticated users who bypass this component receive HTTP 401/403 from
 * the server on every protected API call. The server enforces all auth.
 */
export default function RequireAuth({ children }: Props) {
  const { isAdmin, isLoading } = useAuth();
  const location = useLocation();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <span className="text-text-dim text-sm">Checking session…</span>
      </div>
    );
  }

  if (!isAdmin) {
    return <Navigate to="/admin/login" state={{ from: location }} replace />;
  }

  return <>{children}</>;
}
