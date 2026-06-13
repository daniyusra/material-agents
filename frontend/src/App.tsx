import { BrowserRouter, Routes, Route } from "react-router-dom";
import Layout from "./layouts/Layout";
import LandingPage from "./pages/LandingPage";
import ResearchPage from "./pages/ResearchPage";
import AboutPage from "./pages/AboutPage";
import WhatsAppPage from "./pages/WhatsAppPage";

const WHATSAPP_ENABLED = import.meta.env.VITE_WHATSAPP_ENABLED === "true";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<LandingPage />} />
          <Route path="research" element={<ResearchPage />} />
          {WHATSAPP_ENABLED && <Route path="whatsapp" element={<WhatsAppPage />} />}
          <Route path="about" element={<AboutPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
