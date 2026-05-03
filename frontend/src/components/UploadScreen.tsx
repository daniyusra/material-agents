import { ACCEPTED } from "../types";

interface UploadScreenProps {
  uploading: boolean;
  uploadError: string | null;
  onFile: (file: File) => void;
}

export default function UploadScreen({ uploading, uploadError, onFile }: UploadScreenProps) {
  const onFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) onFile(file);
    e.target.value = "";
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file) onFile(file);
  };

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Material Agents</h1>
      </div>
      <div style={styles.wrapper}>
        <div
          style={styles.dropzone}
          onDragOver={(e) => e.preventDefault()}
          onDrop={onDrop}
        >
          <p style={styles.hint}>Drag &amp; drop a CSV, TSV, or Excel file here</p>
          <p style={styles.or}>or</p>
          <label style={styles.fileLabel}>
            {uploading ? "Uploading…" : "Choose file"}
            <input
              type="file"
              accept={ACCEPTED}
              onChange={onFileInput}
              disabled={uploading}
              style={{ display: "none" }}
            />
          </label>
          {uploadError && <p style={styles.error}>{uploadError}</p>}
        </div>
      </div>
    </div>
  );
}

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    height: "100vh",
    maxWidth: 900,
    margin: "0 auto",
    padding: "0 1rem",
  },
  header: {
    padding: "1rem 0 0.5rem",
  },
  title: {
    fontSize: "1.25rem",
    fontWeight: 600,
    color: "#333",
    margin: 0,
  },
  wrapper: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  dropzone: {
    border: "2px dashed #ccc",
    borderRadius: 16,
    padding: "3rem 2rem",
    textAlign: "center" as const,
    width: "100%",
    maxWidth: 420,
  },
  hint: {
    color: "#555",
    fontSize: "1rem",
    margin: "0 0 0.5rem",
  },
  or: {
    color: "#aaa",
    fontSize: "0.85rem",
    margin: "0.5rem 0",
  },
  fileLabel: {
    display: "inline-block",
    padding: "0.6rem 1.5rem",
    borderRadius: 8,
    background: "#0070f3",
    color: "white",
    fontSize: "0.95rem",
    cursor: "pointer",
    fontWeight: 500,
  },
  error: {
    color: "#d32f2f",
    fontSize: "0.875rem",
    marginTop: "1rem",
  },
} as const;
