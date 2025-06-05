import React from "react";

const InstructionsSection = () => {
  return (
    <div className="instructions-section">
      <h1 className="primary-heading mb-4">
        Instructions to Download Your SUN
      </h1>
      <ol className="instructions-list">
        <li>
          <strong>Single Download Limit:</strong> You can only download your SUN
          ONCE for security reasons. Proceed with caution.
        </li>
        <li>
          <strong>Check Battery Life:</strong> Ensure your device (computer or
          phone) has at least 50% battery life. Verify this via your device's
          battery icon. If you are not sure, PLUG IT IN!!!!
        </li>
        <li>
          <strong>Verify Storage Space:</strong> Confirm you have at least 2GB
          of free storage space, as the SUN file is 1.5GB. Check this in your
          device’s settings under “About” or “Storage.” If needed, delete unused
          files or perform a cleanup to free up space.
        </li>
        <li>
          <strong>Consider Cloud Storage:</strong> You may download your SUN
          directly to a cloud storage service like iCloud, Google Drive, or
          another provider.
        </li>
        <li>
          <strong>Backup Your SUN:</strong> Store a copy of your SUN in a secure
          cloud storage provider to protect against loss of your device. If you
          have a problem with this step ask a friend/family member for help
        </li>
        <li>
          <strong>Test Download Speed:</strong> Check your internet download
          speed by searching for “Download Speed Test” online and running a test
          on your device. Ideal speeds are:
          <ul>
            <li>5–20 MB/s: Consider using a faster connection.</li>
            <li>20–40 MB/s: Solid speed.</li>
            <li>40–100 MB/s: Good speed.</li>
            <li>Above 100 MB/s: Fast.</li>
            <li>Above 1000 MB/s: Very fast.</li>
          </ul>
        </li>
        <li>
          <strong>Stationary Location:</strong> Download your SUN from a
          stationary location to ensure a stable connection.
        </li>
        <li>
          <strong>Initiate Download:</strong> After verifying power, storage,
          and speed, click “Download SUN.” A green checkmark indicates a
          successful download. A red X means the SUN has already been
          downloaded.
        </li>
        <li>
          <strong>Move to Secure Storage:</strong> Once downloaded, verify the
          SUN is in your downloads folder, then move it to a secure location,
          such as a folder or cloud storage.
        </li>
        <li>
          <strong>Store on Computer:</strong> Create a secure folder on your
          computer (e.g., “SUN_Backup” in your Documents directory) and move the
          SUN file there for safekeeping.
        </li>
        <li>
          <strong>Store in Cloud:</strong> Upload the SUN file to your cloud
          storage provider (e.g., iCloud, Google Drive) via their website or
          app, ensuring it is saved in a secure folder.
        </li>
        <li>
          <strong>Explore Your SUN:</strong> Use the Viewer to explore your SUN
          on a floor-by-floor basis. Each floor, identified by Z coordinates,
          contains 1000 x 1000 cells with one million true random numbers.
        </li>
        <li>
          <strong>Create Subsets:</strong> Use the Snippet Creator to generate
          points, lines, or planes from any individual floor of your SUN.
        </li>
      </ol>
    </div>
  );
};

export default InstructionsSection;
