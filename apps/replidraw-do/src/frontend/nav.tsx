import { useEffect, useRef, useState } from "react";
import type { Replicache } from "replicache";
import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import styles from "./nav.module.css";
import { randomShape, Shape, shapeKey, shapePrefix } from "../datamodel/shape";
import { useUserInfo } from "../datamodel/subscriptions";
import type { M } from "../datamodel/mutators";
import { nanoid } from "nanoid";

export function Nav({ rep }: { rep: Replicache<M> }) {
  const [aboutVisible, showAbout] = useState(false);
  const [shareVisible, showShare] = useState(false);
  const urlBox = useRef<HTMLInputElement>(null);
  const userInfo = useUserInfo(rep);

  useEffect(() => {
    if (shareVisible) {
      urlBox.current && urlBox.current.select();
    }
  });

  const onRectangle = () => {
    rep.mutate.createShape(randomShape());
  };

  const duplicateAll = async () => {
    const existingShapes = await rep.query(
      async (tx) =>
        (await tx
          .scan({ prefix: shapePrefix })
          .values()
          .toArray()) as unknown[] as Shape[]
    );
    const newShapes = existingShapes.map((v) => ({
      ...v,
      x: v.x + 5,
      y: v.y + 5,
    }));
    for (const shape of newShapes) {
      await rep.mutate.createShape({ id: nanoid(), shape });
    }
  };

  return (
    <>
      <div className={styles.nav} style={{}}>
        <div
          onClick={() => onRectangle()}
          className={styles.button}
          title="Square"
        >
          <svg
            width="18"
            height="18"
            viewBox="0 0 18 18"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M1 1h16v16H1V1zm1 1h14v14H2V2z"
              fillRule="evenodd"
              fill="white"
            ></path>
          </svg>
        </div>
        {/*
        <div
          className={styles.button}
          title="Clear All"
          onClick={() => rep.mutate.deleteAllShapes()}
        >
          <svg
            width="18"
            height="18"
            viewBox="1 1 14 14"
            xmlns="http://www.w3.org/2000/svg"
            style={{ transform: "rotate(45deg)" }}
          >
            <path
              d="M15 8V7H9V1H8v6H2v1h6v6h1V8h6z"
              fillRule="nonzero"
              fillOpacity="1"
              fill="white"
              stroke="none"
            ></path>
          </svg>
        </div>*/}
        <div className={`${styles.button}`} onClick={() => showShare(true)}>
          Share
        </div>
        <div
          className={`${styles.button} ${styles.about}`}
          onClick={() => showAbout(true)}
        >
          About this Demo
        </div>
        <div className={`${styles.button}`} onClick={() => duplicateAll()}>
          Duplicate All Shapes
        </div>
        <div className={styles.spacer}></div>
        {userInfo && (
          <div
            className={styles.user}
            style={{
              backgroundColor: userInfo.color,
            }}
          >
            {userInfo.avatar} {userInfo.name}
          </div>
        )}
      </div>
      <Modal show={aboutVisible} onHide={() => showAbout(false)} centered>
        <Modal.Header closeButton>
          <Modal.Title>About Replidraw</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <p>
            This is a demonstration of{" "}
            <a href="https://replicache.dev" target="_blank">
              <u>Replicache</u>
            </a>{" "}
            — a JavaScript library that enables realtime, collaborative web apps
            for any backend stack.
          </p>
          <p>
            Try{" "}
            <a href={location.href} target="_blank">
              <u>opening this page</u>
            </a>{" "}
            in two browser windows and moving the boxes around.
          </p>
        </Modal.Body>
        <Modal.Footer>
          <Button
            variant="secondary"
            href="https://github.com/rocicorp/replidraw-do"
            target="_blank"
          >
            Demo Source
          </Button>
          <Button
            variant="primary"
            href="https://replicache.dev/"
            target="_blank"
          >
            Replicache Homepage
          </Button>
        </Modal.Footer>
      </Modal>
      <Modal show={shareVisible} onHide={() => showShare(false)} centered>
        <Modal.Header closeButton>
          <Modal.Title>Share Drawing</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Form>
            <Form.Text>Copy this URL and send to anyone:</Form.Text>
            <Form.Control
              ref={urlBox}
              type="url"
              value={location.href}
              readOnly
            />
          </Form>
        </Modal.Body>
        <Modal.Footer>
          <Button variant="primary" onClick={() => showShare(false)}>
            OK
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
}
