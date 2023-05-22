import styles from './MobileNav.module.css';
import React, {useState} from 'react';
import * as Scroll from 'react-scroll';

let Link = Scroll.Link;

export default function MobileNav() {
  const [toggleMenu, setToggleMenu] = useState(false);
  const toggleNav = () => {
    setToggleMenu(!toggleMenu);
  };

  return (
    <div className={styles.mobileNav}>
      <button
        type="button"
        className={
          toggleMenu ? styles.mobileNavButtonActive : styles.mobileNavButton
        }
        aria-label="Toggle Menu"
        onClick={toggleNav}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
          fill="#2a2c2e"
          className={styles.menuOpen}
        >
          <path
            fillRule="evenodd"
            d="M3 5a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zM3 10a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zM3 15a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1z"
            clipRule="evenodd"
          ></path>
        </svg>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
          fill="#2a2c2e"
          className={styles.menuClose}
        >
          <path
            fillRule="evenodd"
            clipRule="evenodd"
            d="M5.04289 14.9424C4.85536 14.7549 4.75 14.5005 4.75 14.2353C4.75 13.9701 4.85536 13.7157 5.04289 13.5282L8.57843 9.99264L5.04289 6.45711C4.85536 6.26957 4.75 6.01522 4.75 5.75C4.75 5.48478 4.85536 5.23043 5.04289 5.04289C5.23043 4.85536 5.48478 4.75 5.75 4.75C6.01522 4.75 6.26957 4.85536 6.45711 5.04289L9.99264 8.57843L13.5282 5.04289C13.7157 4.85536 13.9701 4.75 14.2353 4.75C14.5005 4.75 14.7549 4.85536 14.9424 5.04289C15.1299 5.23043 15.2353 5.48478 15.2353 5.75C15.2353 6.01522 15.1299 6.26957 14.9424 6.45711L11.4069 9.99264L14.9424 13.5282C15.1299 13.7157 15.2353 13.9701 15.2353 14.2353C15.2353 14.5005 15.1299 14.7549 14.9424 14.9424C14.7549 15.1299 14.5005 15.2353 14.2353 15.2353C13.9701 15.2353 13.7157 15.1299 13.5282 14.9424L9.99264 11.4069L6.45711 14.9424C6.26957 15.1299 6.01522 15.2353 5.75 15.2353C5.48478 15.2353 5.23043 15.1299 5.04289 14.9424Z"
            fill="#2A2C2E"
          />
        </svg>
      </button>

      <div
        className={
          toggleMenu ? styles.mobileNavMenu : styles.mobileNavMenuDisabled
        }
      >
        <ul role="list" className={styles.navArray}>
          <li>
            <Link
              href="#"
              className={styles.navLink}
              activeClass={styles.navLinkActive}
              to="benefits"
              offset={100}
              smooth={true}
              duration={250}
              onClick={toggleNav}
            >
              Benefits
            </Link>
          </li>
          <li>
            <Link
              href="#"
              className={styles.navLink}
              activeClass={styles.navLinkActive}
              to="how"
              offset={100}
              smooth={true}
              duration={250}
              onClick={toggleNav}
            >
              How it Works
            </Link>
          </li>
          <li>
            <Link
              href="#"
              className={styles.navLink}
              activeClass={styles.navLinkActive}
              to="customers"
              offset={116}
              smooth={true}
              duration={250}
              onClick={toggleNav}
            >
              Customers
            </Link>
          </li>
          <li>
            <Link
              href="#"
              className={styles.navLink}
              activeClass={styles.navLinkActive}
              to="pricing"
              offset={100}
              spy={false}
              smooth={true}
              duration={250}
              onClick={toggleNav}
            >
              Pricing
            </Link>
          </li>
          <li>
            <Link
              href="#"
              className={styles.buttonPrimary}
              to="get-started"
              smooth={true}
              offset={100}
              duration={250}
              onClick={toggleNav}
            >
              Get started
            </Link>
          </li>
        </ul>
      </div>
    </div>
  );
}
