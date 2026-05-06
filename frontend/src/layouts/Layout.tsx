import { Outlet } from "react-router-dom";
import Nav from "../components/Nav";

export default function Layout() {
  return (
    <>
      <style>{`
        .layout-main {
          padding-top: var(--nav-h);
          min-height: 100vh;
        }
      `}</style>
      <Nav />
      <main className="layout-main">
        <Outlet />
      </main>
    </>
  );
}
