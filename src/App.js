import "./App.css";
import Navbar from "./Components/Navbar";
import { Route, Routes } from "react-router-dom";
import Home from "./Components/Pages/Home";
import About from "./Components/Pages/About";
import Partners from "./Components/Pages/Partners";
import Applications from "./Components/Pages/Applications";
import Footer from "./Components/Footer";
import Technical_papers from "./Components/Pages/Technical_papers";
import Book from "./Components/Pages/Book";
import Contact from "./Components/Pages/Contact";
import Career from "./Components/Pages/Career";
import Login from "./Components/Pages/Login";
import Purchase from "./Components/Pages/Purchase";
import Guarantee from "./Components/Pages/Guarantee";
import Account from "./Components/Pages/Account";
import Store from "./Components/Pages/Store";
import CartProvider from "./Components/Pages/CartContext";
import PurchaseTP from "./Components/Pages/PurchaseTP";
import PurchaseForm from "./Components/Pages/PurchaseForm";
import Application_download from "./Components/Pages/Application_download";
import Terms from "./Components/Pages/Terms";
import CubeDisplay from "./Components/Pages/CubeDisplay";
import SerialKeyVerification from "./Components/Pages/SerialKeyVerification";
import CubeComponent from "./Components/Pages/CubeComponent";

function App() {
  return (
    <div className="App">
      <CartProvider>
        <Navbar />
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/home" element={<Home />} />
          <Route path="/about" element={<About />} />
          <Route path="/partners" element={<Partners />} />
          <Route path="/applications" element={<Applications />} />
          <Route path="/technical_papers" element={<Technical_papers />} />
          <Route path="/book" element={<Book />} />
          <Route path="/contact" element={<Contact />} />
          <Route path="/career" element={<Career />} />
          <Route path="/login" element={<Account />} />
          <Route path="/purchase" element={<Store />} />
          <Route path="/terms" element={<Terms />} />
          <Route path="/cube" element={<CubeDisplay />} />
          <Route path="/verify" element={<SerialKeyVerification />} />
          <Route path="/cube2" element={<CubeComponent />} />

          <Route path="/guarantee" element={<Guarantee />} />
          <Route path="/purchaseTP" element={<PurchaseTP />} />
          <Route path="/purchaseform" element={<PurchaseForm />} />
          <Route
            path="/applicationDownload"
            element={<Application_download />}
          />
        </Routes>
        <Footer />
      </CartProvider>
    </div>
  );
}

export default App;
