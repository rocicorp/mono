export default function IssuePage() {
  return (
    <div className="issue-detail-container">
      {/* Center column of info */}
      <div className="issue-detail">
        <div className="issue-breadcrumb">
          <span className="breadcrumb-item">Open issues</span>
          <span className="breadcrumb-item">&rarr;</span>
          <span className="breadcrumb-item">ZB-15</span>
        </div>
        <h1>Issue Title</h1>
      </div>

      {/* Right sidebar */}
      <div className="issue-sidebar"></div>
    </div>
  );
}
