import { BrowserRouter, Routes, Route } from "react-router-dom";
import Layout from "./layouts/Layout";
import LandingPage from "./pages/LandingPage";
import ResearchPage from "./pages/ResearchPage";
import AboutPage from "./pages/AboutPage";
import WhatsAppPage from "./pages/WhatsAppPage";
import BlogListPage from "./pages/blog/BlogListPage";
import BlogArticlePage from "./pages/blog/BlogArticlePage";
import AdminLoginPage from "./pages/admin/AdminLoginPage";
import AdminDashboard from "./pages/admin/AdminDashboard";
import ArticleEditor from "./pages/admin/ArticleEditor";
import RequireAuth from "./components/blog/RequireAuth";
import { AuthProvider } from "./contexts/AuthContext";

const WHATSAPP_ENABLED = import.meta.env.VITE_WHATSAPP_ENABLED === "true";
const BLOG_ENABLED = import.meta.env.VITE_BLOG_ENABLED === "true";

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<LandingPage />} />
            <Route path="research" element={<ResearchPage />} />
            {WHATSAPP_ENABLED && <Route path="whatsapp" element={<WhatsAppPage />} />}
            <Route path="about" element={<AboutPage />} />

            {/* Blog — gated on VITE_BLOG_ENABLED */}
            {BLOG_ENABLED && (
              <>
                <Route path="blog" element={<BlogListPage />} />
                <Route path="blog/:slug" element={<BlogArticlePage />} />

                {/* Admin — all protected by RequireAuth (UX guard; server enforces auth) */}
                <Route path="admin/login" element={<AdminLoginPage />} />
                <Route
                  path="admin"
                  element={
                    <RequireAuth>
                      <AdminDashboard />
                    </RequireAuth>
                  }
                />
                <Route
                  path="admin/new"
                  element={
                    <RequireAuth>
                      <ArticleEditor />
                    </RequireAuth>
                  }
                />
                <Route
                  path="admin/edit/:id"
                  element={
                    <RequireAuth>
                      <ArticleEditor />
                    </RequireAuth>
                  }
                />
              </>
            )}
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}
